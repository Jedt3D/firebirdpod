import 'dart:typed_data';

import 'package:firebirdpod/firebirdpod.dart';
import 'package:serverpod_auth_core_server/serverpod_auth_core_server.dart'
    as auth;
import 'package:serverpod_auth_idp_server/serverpod_auth_idp_server.dart'
    as idp;
import 'package:serverpod_database/serverpod_database.dart';
import 'package:test/test.dart';

import 'firebird_serverpod_module_test_support.dart';
import 'firebird_test_support.dart';

void main() {
  group('Firebird serverpod_auth_idp module', () {
    test(
      'schema round-trips and persists full auth IDP indexed-text rows',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          markTestSkipped(
            'set FIREBIRDPOD_RUN_FBCLIENT_DIRECT=1 to run live Firebird module tests',
          );
          return;
        }

        registerFirebirdServerpodDialect();

        final poolManager = FirebirdServerpodPoolManager(
          idp.Protocol(),
          null,
          FirebirdServerpodDatabaseConfig(
            host: 'localhost',
            port: 3050,
            user: firebirdTestUser(),
            password: firebirdTestPassword(),
            name: firebirdTestDatabasePath(),
            charset: 'UTF8',
            fbClientLibraryPath: firebirdClientLibraryPath(),
          ),
        )..start();
        addTearDown(poolManager.stop);

        late Database database;
        final session = _TestSession(() => database);
        database = DatabaseConstructor.create(
          session: session,
          poolManager: poolManager,
        );

        final targetTables = poolManager.serializationManager
            .getTargetTableDefinitions();
        await cleanupModuleArtifacts(session, targetTables);
        addTearDown(() => cleanupModuleArtifacts(session, targetTables));

        final definition = DatabaseDefinition(
          moduleName: poolManager.serializationManager.getModuleName(),
          tables: targetTables,
          installedModules: const [],
          migrationApiVersion: 1,
        );
        final definitionSql = const FirebirdServerpodSqlGenerator()
            .generateDatabaseDefinitionSql(
              definition,
              installedModules: const [],
            );

        await session.db.unsafeSimpleExecute(definitionSql);

        expect(await MigrationManager.verifyDatabaseIntegrity(session), isTrue);

        final authUser = await session.db.insertRow<auth.AuthUser>(
          auth.AuthUser(scopeNames: {'profile'}),
        );

        final maxIndexedEmail = _buildMaxIndexedEmail();
        final appleIdentifier = _repeat('A', 512);
        final facebookIdentifier = _repeat('F', 512);
        final firebaseIdentifier = _repeat('I', 512);
        final githubIdentifier = _repeat('G', 512);
        final googleIdentifier = _repeat('O', 512);
        final microsoftIdentifier = _repeat('M', 512);
        final passkeyKeyIdBase64 = _repeat('K', 512);
        final rateLimitDomain = _repeat('d', 96);
        final rateLimitSource = _repeat('s', 96);
        final rateLimitNonce = _repeat('n', 255);

        final anonymousAccount = await idp.AnonymousAccount.db.insertRow(
          session,
          idp.AnonymousAccount(authUserId: authUser.id!),
        );
        final fetchedAnonymousAccount = await idp.AnonymousAccount.db.findById(
          session,
          anonymousAccount.id!,
          include: idp.AnonymousAccount.include(
            authUser: auth.AuthUser.include(),
          ),
        );
        expect(fetchedAnonymousAccount, isNotNull);
        expect(fetchedAnonymousAccount!.authUser?.id, authUser.id);

        final secretChallenge = await idp.SecretChallenge.db.insertRow(
          session,
          idp.SecretChallenge(challengeCodeHash: 'argon2id\$challenge'),
        );
        final emailAccountRequest = await idp.EmailAccountRequest.db.insertRow(
          session,
          idp.EmailAccountRequest(
            email: maxIndexedEmail,
            challengeId: secretChallenge.id!,
          ),
        );
        final fetchedEmailAccountRequest = await idp.EmailAccountRequest.db
            .findFirstRow(
              session,
              where: (table) => table.email.equals(maxIndexedEmail),
              include: idp.EmailAccountRequest.include(
                challenge: idp.SecretChallenge.include(),
              ),
            );
        expect(fetchedEmailAccountRequest, isNotNull);
        expect(fetchedEmailAccountRequest!.id, emailAccountRequest.id);
        expect(fetchedEmailAccountRequest.challenge?.id, secretChallenge.id);
        expect(fetchedEmailAccountRequest.email, maxIndexedEmail);

        final emailAccount = await idp.EmailAccount.db.insertRow(
          session,
          idp.EmailAccount(
            authUserId: authUser.id!,
            email: maxIndexedEmail,
            passwordHash: 'argon2id\$account',
          ),
        );
        final fetchedEmailAccount = await idp.EmailAccount.db.findFirstRow(
          session,
          where: (table) => table.email.equals(maxIndexedEmail),
          include: idp.EmailAccount.include(authUser: auth.AuthUser.include()),
        );
        expect(fetchedEmailAccount, isNotNull);
        expect(fetchedEmailAccount!.id, emailAccount.id);
        expect(fetchedEmailAccount.authUser?.id, authUser.id);
        expect(fetchedEmailAccount.email, maxIndexedEmail);

        final emailPasswordResetRequest = await idp
            .EmailAccountPasswordResetRequest
            .db
            .insertRow(
              session,
              idp.EmailAccountPasswordResetRequest(
                emailAccountId: emailAccount.id!,
                challengeId: secretChallenge.id!,
              ),
            );
        final fetchedEmailPasswordResetRequest = await idp
            .EmailAccountPasswordResetRequest
            .db
            .findById(session, emailPasswordResetRequest.id!);
        expect(fetchedEmailPasswordResetRequest, isNotNull);
        expect(
          fetchedEmailPasswordResetRequest!.emailAccountId,
          emailAccount.id,
        );

        final passkeyChallenge = await idp.PasskeyChallenge.db.insertRow(
          session,
          idp.PasskeyChallenge(challenge: ByteData(32)),
        );
        final fetchedPasskeyChallenge = await idp.PasskeyChallenge.db.findById(
          session,
          passkeyChallenge.id!,
        );
        expect(fetchedPasskeyChallenge, isNotNull);
        expect(fetchedPasskeyChallenge!.challenge.lengthInBytes, 32);

        final appleAccount = await idp.AppleAccount.db.insertRow(
          session,
          idp.AppleAccount(
            authUserId: authUser.id!,
            userIdentifier: appleIdentifier,
            refreshToken: 'refresh-token',
            refreshTokenRequestedWithBundleIdentifier: true,
            email: maxIndexedEmail,
          ),
        );
        final fetchedAppleAccount = await idp.AppleAccount.db.findFirstRow(
          session,
          where: (table) => table.userIdentifier.equals(appleIdentifier),
          include: idp.AppleAccount.include(authUser: auth.AuthUser.include()),
        );
        expect(fetchedAppleAccount?.id, appleAccount.id);
        expect(fetchedAppleAccount?.authUser?.id, authUser.id);

        final facebookAccount = await idp.FacebookAccount.db.insertRow(
          session,
          idp.FacebookAccount(
            authUserId: authUser.id!,
            userIdentifier: facebookIdentifier,
            email: 'facebook@example.com',
          ),
        );
        final fetchedFacebookAccount = await idp.FacebookAccount.db
            .findFirstRow(
              session,
              where: (table) => table.userIdentifier.equals(facebookIdentifier),
            );
        expect(fetchedFacebookAccount?.id, facebookAccount.id);

        final firebaseAccount = await idp.FirebaseAccount.db.insertRow(
          session,
          idp.FirebaseAccount(
            authUserId: authUser.id!,
            userIdentifier: firebaseIdentifier,
            email: 'firebase@example.com',
          ),
        );
        final fetchedFirebaseAccount = await idp.FirebaseAccount.db
            .findFirstRow(
              session,
              where: (table) => table.userIdentifier.equals(firebaseIdentifier),
            );
        expect(fetchedFirebaseAccount?.id, firebaseAccount.id);

        final githubAccount = await idp.GitHubAccount.db.insertRow(
          session,
          idp.GitHubAccount(
            authUserId: authUser.id!,
            userIdentifier: githubIdentifier,
            email: 'github@example.com',
          ),
        );
        final fetchedGitHubAccount = await idp.GitHubAccount.db.findFirstRow(
          session,
          where: (table) => table.userIdentifier.equals(githubIdentifier),
        );
        expect(fetchedGitHubAccount?.id, githubAccount.id);

        final googleAccount = await idp.GoogleAccount.db.insertRow(
          session,
          idp.GoogleAccount(
            authUserId: authUser.id!,
            email: 'google@example.com',
            userIdentifier: googleIdentifier,
          ),
        );
        final fetchedGoogleAccount = await idp.GoogleAccount.db.findFirstRow(
          session,
          where: (table) => table.userIdentifier.equals(googleIdentifier),
        );
        expect(fetchedGoogleAccount?.id, googleAccount.id);

        final microsoftAccount = await idp.MicrosoftAccount.db.insertRow(
          session,
          idp.MicrosoftAccount(
            authUserId: authUser.id!,
            userIdentifier: microsoftIdentifier,
            email: 'microsoft@example.com',
          ),
        );
        final fetchedMicrosoftAccount = await idp.MicrosoftAccount.db
            .findFirstRow(
              session,
              where: (table) =>
                  table.userIdentifier.equals(microsoftIdentifier),
            );
        expect(fetchedMicrosoftAccount?.id, microsoftAccount.id);

        final passkeyAccount = await idp.PasskeyAccount.db.insertRow(
          session,
          idp.PasskeyAccount(
            authUserId: authUser.id!,
            keyId: ByteData(32),
            keyIdBase64: passkeyKeyIdBase64,
            clientDataJSON: ByteData(64),
            attestationObject: ByteData(64),
            originalChallenge: ByteData(32),
          ),
        );
        final fetchedPasskeyAccount = await idp.PasskeyAccount.db.findFirstRow(
          session,
          where: (table) => table.keyIdBase64.equals(passkeyKeyIdBase64),
          include: idp.PasskeyAccount.include(
            authUser: auth.AuthUser.include(),
          ),
        );
        expect(fetchedPasskeyAccount?.id, passkeyAccount.id);
        expect(fetchedPasskeyAccount?.authUser?.id, authUser.id);

        final rateLimitedAttempt = await idp.RateLimitedRequestAttempt.db
            .insertRow(
              session,
              idp.RateLimitedRequestAttempt(
                domain: rateLimitDomain,
                source: rateLimitSource,
                nonce: rateLimitNonce,
                ipAddress: '127.0.0.1',
                extraData: {'provider': 'email'},
              ),
            );
        final fetchedRateLimitedAttempt = await idp.RateLimitedRequestAttempt.db
            .findFirstRow(
              session,
              where: (table) =>
                  table.domain.equals(rateLimitDomain) &
                  table.source.equals(rateLimitSource) &
                  table.nonce.equals(rateLimitNonce),
            );
        expect(fetchedRateLimitedAttempt?.id, rateLimitedAttempt.id);
        expect(fetchedRateLimitedAttempt?.extraData, {'provider': 'email'});
      },
    );
  });
}

class _TestSession implements DatabaseSession {
  _TestSession(this._database);

  final Database Function() _database;

  @override
  Database get db => _database();

  @override
  Transaction? get transaction => null;

  @override
  LogQueryFunction? get logQuery => null;

  @override
  LogWarningFunction? get logWarning => null;
}

String _buildMaxIndexedEmail() =>
    '${_repeat('u', 64)}@${_repeat('a', 63)}.${_repeat('b', 63)}.${_repeat('c', 63)}.${_repeat('d', 63)}';

String _repeat(String character, int count) =>
    List.filled(count, character).join();
