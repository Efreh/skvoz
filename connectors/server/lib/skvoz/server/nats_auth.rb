# frozen_string_literal: true

# Native NATS NKey/JWT/XKey formats; cryptographic operations use libsodium.
require 'base64'
require 'digest'
require 'json'
require 'rbnacl'
require 'securerandom'

module Skvoz
  module Server
    module NatsAuth
      ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567'
      PREFIX = { account: 0, server: 104, user: 160, curve: 184 }.freeze

      abort 'Exact JSON3.0.2 parser required' unless JSON::VERSION == '3.0.2'

      def self.json(text)
        JSON.parse(text, max_nesting: 32, allow_duplicate_key: false)
      rescue JSON::JSONError
        raise ArgumentError, 'Invalid JSON envelope'
      end
      def self.b64(bytes) = Base64.urlsafe_encode64(bytes, padding: false)

      def self.unb64(text)
        raise ArgumentError, 'Noncanonical base64url' unless text.match?(/\A[A-Za-z0-9_-]*\z/)
        bytes = Base64.urlsafe_decode64(text)
        raise ArgumentError, 'Noncanonical base64url' unless b64(bytes) == text
        bytes
      end

      def self.crc16(bytes)
        bytes.bytes.reduce(0) do |crc, byte|
          crc ^= byte << 8
          8.times { crc = ((crc << 1) ^ ((crc & 0x8000).zero? ? 0 : 0x1021)) & 0xffff }
          crc
        end
      end

      def self.base32(bytes)
        bits = bytes.unpack1('B*')
        bits += '0' * ((5 - bits.size % 5) % 5)
        bits.scan(/.{5}/).map { |part| ALPHABET[part.to_i(2)] }.join
      end

      def self.encode(prefix, raw)
        bytes = prefix.chr.b + raw.b
        base32(bytes + [crc16(bytes)].pack('v'))
      end

      def self.decode(text, kind, seed: false)
        raise ArgumentError, 'Invalid NKey alphabet' unless text.is_a?(String) && text.match?(/\A[A-Z2-7]+\z/)
        bits = text.chars.map { |char| ALPHABET.index(char).to_s(2).rjust(5, '0') }.join
        bytes = [bits[0, bits.size / 8 * 8]].pack('B*')
        raise ArgumentError, 'Noncanonical NKey' unless base32(bytes) == text
        length = seed ? 36 : 35
        raise ArgumentError, 'Invalid NKey length' unless bytes.bytesize == length
        body, checksum = bytes[0...-2], bytes[-2..].unpack1('v')
        raise ArgumentError, 'Invalid NKey checksum' unless crc16(body) == checksum
        if seed
          a, b = body.bytes.first(2)
          prefix = ((a & 7) << 5) | (b >> 3)
          raise ArgumentError, 'Invalid seed type' unless (a & 0xf8) == 144 && (b & 7).zero? && prefix == PREFIX.fetch(kind)
          body[2..]
        else
          raise ArgumentError, 'Invalid public key type' unless body.getbyte(0) == PREFIX.fetch(kind)
          body[1..]
        end
      end

      class Key
        attr_reader :public_key, :kind, :seed
        def initialize(kind, seed = nil)
          @kind = kind
          raw = seed ? NatsAuth.decode(seed, kind, seed: true) : SecureRandom.random_bytes(32)
          prefix = PREFIX.fetch(kind)
          encoded = [144 | (prefix >> 5), (prefix & 31) << 3].pack('CC') + raw
          @seed = NatsAuth.base32(encoded + [NatsAuth.crc16(encoded)].pack('v'))
          @key = kind == :curve ? RbNaCl::PrivateKey.new(raw) : RbNaCl::SigningKey.new(raw)
          public_raw = kind == :curve ? @key.public_key.to_s : @key.verify_key.to_s
          @public_key = NatsAuth.encode(PREFIX.fetch(kind), public_raw)
        end

        def jwt(claims)
          header = NatsAuth.b64(JSON.generate(typ: 'JWT', alg: 'ed25519-nkey'))
          payload = NatsAuth.b64(JSON.generate(claims.merge('iss' => public_key)))
          input = "#{header}.#{payload}"
          "#{input}.#{NatsAuth.b64(@key.sign(input))}"
        end

        def seal(text, remote)
          nonce = SecureRandom.random_bytes(24)
          box = RbNaCl::Box.new(NatsAuth.decode(remote, :curve), @key)
          'xkv1'.b + nonce + box.encrypt(nonce, text.b)
        end

        def open(bytes, remote)
          raise ArgumentError, 'Encrypted XKey envelope required' unless bytes.bytesize >= 44 && bytes[0, 4] == 'xkv1'
          RbNaCl::Box.new(NatsAuth.decode(remote, :curve), @key).decrypt(bytes[4, 24], bytes[28..])
        end
      end

      def self.verify_jwt(token, kind: :server)
        raise ArgumentError, 'JWT size limit' unless token.bytesize <= 16_384
        parts = token.split('.', -1)
        raise ArgumentError, 'Invalid JWT envelope' unless parts.size == 3
        header = json(unb64(parts[0]))
        raise ArgumentError, 'JWT algorithm/type mismatch' unless header == { 'typ' => 'JWT', 'alg' => 'ed25519-nkey' }
        claims = json(unb64(parts[1]))
        key = RbNaCl::VerifyKey.new(decode(claims.fetch('iss'), kind))
        key.verify(unb64(parts[2]), parts[0..1].join('.'))
        claims
      end

      def self.request(bytes, xkey, curve, issuer)
        raise ArgumentError, 'Auth record exceeds limit' unless bytes.is_a?(String) && bytes.bytesize <= 16_384
        claims = verify_jwt(curve.open(bytes, xkey))
        fields = claims.fetch('nats')
        server = fields.fetch('server_id')
        now = Time.now.to_i
        raise ArgumentError, 'Request audience/subject mismatch' unless claims['aud'] == 'nats-authorization-request' && claims['sub'] == issuer
        raise ArgumentError, 'Request signer/server mismatch' unless claims['iss'] == server.fetch('id') && xkey == server.fetch('xkey')
        raise ArgumentError, 'Request time mismatch' unless claims['iat'].is_a?(Integer) && claims['exp'].is_a?(Integer) && claims['iat'] <= now + 2 && claims['exp'] > now && claims['exp'] <= now + 5
        raise ArgumentError, 'Request type/version mismatch' unless fields['type'] == 'authorization_request' && fields['version'] == 2
        decode(fields.fetch('user_nkey'), :user)
        raise ArgumentError, 'Standard client required' unless fields.fetch('client_info')['kind'] == 'Client'
        # Bound native CONNZ metadata, independently of the encrypted request cap.
        [fields.fetch('client_info'), fields.fetch('connect_opts')].each do |metadata|
          %w[name lang version].each do |key|
            value = metadata[key]
            raise ArgumentError, 'Client metadata exceeds limit' unless value.nil? || value.is_a?(String) && value.bytesize <= 128
          end
        end
        cid = fields.fetch('client_info').fetch('id')
        raise ArgumentError, 'Invalid broker connection ID' unless cid.is_a?(Integer) && cid.positive? && cid < 1 << 64
        claims
      end

      def self.response(request, issuer, curve, identity: nil, permissions: nil, error: nil)
        fields = request.fetch('nats')
        user = fields.fetch('user_nkey')
        server = fields.fetch('server_id').fetch('id')
        now = Time.now.to_i
        outer = { 'sub' => user, 'aud' => server, 'iat' => now,
                  'exp' => [now + 2, request.fetch('exp')].min,
                  'nats' => { 'type' => 'authorization_response', 'version' => 2 } }
        if error
          outer['nats']['error'] = error
        else
          inner = issuer.jwt('sub' => user, 'aud' => 'APP', 'iat' => now, 'name' => identity,
                             'nats' => { 'type' => 'user', 'version' => 2,
                                         'pub' => { 'allow' => permissions.fetch('publish') },
                                         'sub' => { 'allow' => permissions.fetch('subscribe') },
                                         'subs' => 32, 'data' => -1, 'payload' => 65_588,
                                         'allowed_connection_types' => ['STANDARD'] })
          outer['nats']['jwt'] = inner
        end
        body = curve.seal(issuer.jwt(outer), fields.fetch('server_id').fetch('xkey'))
        raise ArgumentError, 'Auth response exceeds limit' if body.bytesize > 65_588
        body
      end
    end
  end
end
