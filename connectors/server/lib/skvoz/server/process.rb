# frozen_string_literal: true
require 'rbconfig'
require 'fileutils'
require 'fcntl'
require_relative 'errors'

module Skvoz
  module Server
    module PrivateFiles
      def self.ancestors(path, private_leaf: false)
        raise Error, 'Private directory must be absolute' unless path.start_with?('/')
        current = '/'
        path.split('/').reject(&:empty?).each do |part|
          raise Error, 'Unsafe private directory' if %w[. ..].include?(part)
          current = File.join(current, part)
          stat = File.lstat(current)
          if stat.symlink? || !stat.directory? || ![0, Process.euid].include?(stat.uid) || ((stat.mode & 0o022).positive? && (stat.mode & 0o1000).zero?)
            raise Error, 'Unsafe private directory'
          end
        end
        stat = File.stat(path)
        if private_leaf && !(stat.uid == Process.euid && stat.mode & 0o777 == 0o700)
          raise Error, 'Private directory permissions required'
        end
        path
      end

      def self.directory(path) = ancestors(path, private_leaf: true)

      def self.create_directory(path)
        ancestors(File.dirname(path))
        Dir.mkdir(path, 0o700) unless File.exist?(path)
        directory(path)
      end

      def self.read(path, maximum: 65_536)
        directory(File.dirname(path))
        before = File.lstat(path)
        raise Error, 'Unsafe private file' unless before.file? && before.uid == Process.euid && before.mode & 0o177 == 0 && before.size <= maximum
        File.open(path, File::RDONLY | File::NOFOLLOW) do |file|
          stat = file.stat
          raise Error, 'Private file changed during open' unless stat.dev == before.dev && stat.ino == before.ino
          bytes = file.read(maximum + 1)
          raise Error, 'Private file exceeds limit' if bytes.bytesize > maximum
          bytes
        end
      rescue Errno::ELOOP
        raise Error, 'Unsafe private file'
      end

      def self.write(path, bytes)
        directory(File.dirname(path))
        temporary = path + ".tmp-#{Process.pid}-#{rand(1 << 32)}"
        begin
          File.open(temporary, File::WRONLY | File::CREAT | File::EXCL | File::NOFOLLOW, 0o600) do |file|
            file.write(bytes)
            file.flush
            file.fsync
          end
          File.rename(temporary, path)
          File.open(File.dirname(path), File::RDONLY) { |file| file.fsync }
        ensure
          File.unlink(temporary) if File.exist?(temporary)
        end
      end
    end

    class ChildProcess
      attr_reader :pid, :status, :start_time, :output, :output_overflow

      def exit_category
        return nil if @status.nil?
        return "child_reaped" if @status == :reaped
        @status.signaled? ? "#{@label}_signal_#{@status.termsig}" : "#{@label}_exit_#{@status.exitstatus}"
      end

      def initialize(argv, label:, capture: 0, descriptors: {})
        @argv, @label, @capture, @descriptors = argv, label, capture, descriptors
        @output = +''.b
        @output_overflow = false
      end

      def start(task)
        raise Error, 'Child spawn must run on host main thread' unless Thread.current == Thread.main
        reader, writer = IO.pipe
        guard = File.expand_path('../../../bin/skvoz-child', __dir__)
        @pid = Process.spawn('setpriv', '--pdeathsig', 'KILL', RbConfig.ruby, guard, Process.pid.to_s, @descriptors.keys.join(','), *@argv,
                             **@descriptors, pgroup: true, close_others: true, in: File::NULL, out: writer, err: writer)
        writer.close
        @start_time = self.class.identity(@pid)&.fetch(:start)
        @drain = task.async do
          loop do
            chunk = reader.readpartial(4096)
            if @capture.positive?
              available = @capture - @output.bytesize
              @output << chunk.byteslice(0, available) if available.positive?
              if chunk.bytesize > available
                @output_overflow = true
                signal('KILL')
              end
            end
          end
        rescue EOFError, IOError
          nil
        ensure
          reader.close unless reader.closed?
        end
        self
      rescue StandardError
        reader&.close
        writer&.close
        raise
      end

      def self.identity(pid)
        fields = File.read("/proc/#{pid}/stat").split(') ', 2).last.split
        { state: fields[0], start: fields[19] }
      rescue Errno::ENOENT, Errno::ESRCH
        nil
      end

      def alive?
        return false if @status
        result = Process.waitpid2(@pid, Process::WNOHANG)
        @status = result.last if result
        @status.nil?
      rescue Errno::ECHILD
        @status ||= :reaped
        false
      end

      def signal(name)
        Process.kill(name, -@pid) if alive?
      rescue Errno::ESRCH
        nil
      end

      def stop(deadline)
        signal('TERM')
        Async::Task.current.sleep(0.02) while alive? && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline - 0.25
        signal('KILL') if alive?
        Async::Task.current.sleep(0.01) while alive? && Process.clock_gettime(Process::CLOCK_MONOTONIC) < deadline
        raise Error, 'Owned child reaping deadline exceeded' if alive?
        @drain&.stop if @drain&.alive?
      rescue Errno::ECHILD
        nil
      end
    end
  end
end
