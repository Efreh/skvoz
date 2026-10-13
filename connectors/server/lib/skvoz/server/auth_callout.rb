# frozen_string_literal: true
require 'bcrypt'
require 'thread'
require_relative 'nats_auth'
require_relative 'nats_connection'

module Skvoz
  module Server
    class AuthCallout
      attr_reader :failure, :overloaded

      def initialize(config, state, revocation)
        @config, @state, @revocation = config, state, revocation
        @issuer = NatsAuth::Key.new(:account, state.value.fetch('auth').fetch('issuer_seed'))
        @curve = NatsAuth::Key.new(:curve, state.value.fetch('auth').fetch('xkey_seed'))
        @jobs, @results = SizedQueue.new(16), SizedQueue.new(16)
        @overloaded = 0
        @paused = false
        @running = false
      end

      def start(task)
        @failure = nil
        @running = true
        @connection = NatsConnection.new(@config, @state, 'auth', max_body: 20_480, max_publish: 65_588).start(task)
        @connection.subscribe('$SYS.REQ.USER.AUTH') { |message| receive(message) }
        @worker = Thread.new { password_work }
        @drain = task.async do
          while @running
            begin
              result = @results.pop(true)
              result.fetch(:complete).call(result)
            rescue ThreadError
              task.sleep(0.01)
            end
          end
        rescue StandardError => error
          @failure = 'auth_responder_unavailable'
          Diagnostics.emit(@failure, **Diagnostics.error_fields(error))
        end
        self
      rescue StandardError
        stop
        raise
      end

      def ready? = @running && @failure.nil? && @connection&.ready? && @worker&.alive? && @drain&.alive?
      def pause! = @paused = true
      def resume! = @paused = false

      def hash_password(password)
        raise Error, 'Password must contain 12 to 72 UTF-8 bytes without NUL' unless State.password?(password)
        perform(:hash, password: password)
      end

      def verify_password(login, password)
        user = @state.value.fetch('users')[login]
        return false unless user && State.password?(password)
        revision = user.fetch('access_revision')
        verified = perform(:verify, password: password, hash: user.fetch('hash'))
        verified && @state.value.fetch('users').dig(login, 'access_revision') == revision
      end

      def stop
        @running = false
        @connection&.stop
        @drain.stop if @drain&.alive? && @drain != Async::Task.current
        @jobs.clear
        @jobs.push(nil, true) rescue ThreadError
        if @worker
          @worker.join(2)
          @worker.kill if @worker.alive?
          @worker.join(0.1)
          raise Error, 'Password worker retirement unconfirmed' if @worker.alive?
        end
        @results.clear
      end

      private

      def password_work
        while (job = @jobs.pop)
          result = nil
          begin
            if Process.clock_gettime(Process::CLOCK_MONOTONIC) <= job.fetch(:deadline)
              result = if job.fetch(:type) == :hash
                         State.password_hash(job.fetch(:password))
                       else
                         job[:hash] && BCrypt::Password.new(job.fetch(:hash)) == job.fetch(:password)
                       end
            end
          rescue ArgumentError, BCrypt::Errors::InvalidHash
            result = false
          ensure
            job.delete(:password)
            job.delete(:hash)
          end
          @results.push(job.merge(result: result))
        end
      rescue StandardError
        @failure = 'password_worker_unavailable' if @running
      end

      def enqueue(job)
        @jobs.push(job.merge(deadline: Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2), true)
        true
      rescue ThreadError
        @overloaded += 1
        false
      end

      def perform(type, **arguments)
        raise Error, 'Password worker unavailable' unless ready?
        changed = Async::Condition.new
        complete = false
        result = nil
        accepted = enqueue(arguments.merge(type: type, complete: lambda do |job|
          result, complete = job[:result], true
          changed.signal
        end))
        arguments.clear
        raise Error, 'Password work overloaded' unless accepted
        Async::Task.current.with_timeout(2) { changed.wait until complete || !ready? }
        raise Error, 'Password work unavailable' unless complete && !result.nil?
        result
      end

      def receive(message)
        reply = message[:reply]
        raise ArgumentError, 'Invalid native auth reply subject' unless reply.is_a?(String) && reply.match?(/\A(?:\$SYS\.)?_INBOX\.[a-zA-Z0-9_.-]{1,512}\z/)
        request = NatsAuth.request(message.fetch(:data), message.fetch(:headers).fetch('Nats-Server-Xkey'), @curve, @issuer.public_key)
        credentials = request.fetch('nats').delete('connect_opts')
        login, password = credentials.values_at('user', 'pass')
        unless !@paused && @state.access_ready? && State.login?(login) && State.password?(password)
          return respond(reply, request, error: 'Authentication rejected')
        end
        user = @state.value.fetch('users')[login]
        revision = user&.fetch('access_revision')
        accepted = enqueue(type: :verify, password: password, hash: user&.fetch('hash'), complete: lambda do |job|
          current = @state.value.fetch('users')[login]
          valid = job[:result] && Process.clock_gettime(Process::CLOCK_MONOTONIC) <= job.fetch(:deadline) &&
                  !@paused && @state.access_ready? && current && current['access_revision'] == revision && request['exp'] > Time.now.to_i
          if valid
            identity = @state.identity(login)
            unless @revocation.reserve(request, identity)
              next respond(reply, request, error: 'Authentication overloaded')
            end
            respond(reply, request, identity: identity, permissions: @state.permissions(login))
          else
            respond(reply, request, error: 'Authentication rejected')
          end
        end)
        respond(reply, request, error: 'Authentication overloaded') unless accepted
      rescue ArgumentError, KeyError, TypeError, NoMethodError, RbNaCl::CryptoError
        # Malformed signed/encrypted requests get no credential-bearing response.
        nil
      ensure
        # Completion closures must not retain CONNECT credentials in their environment.
        password = credentials = message = nil
      end

      def respond(reply, request, **arguments)
        body = NatsAuth.response(request, @issuer, @curve, **arguments)
        @connection.publish(reply, body)
      end
    end
  end
end
