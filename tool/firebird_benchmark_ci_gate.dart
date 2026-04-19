import 'dart:convert';
import 'dart:io';

import 'package:firebirdpod/firebirdpod.dart';

Future<void> main(List<String> arguments) async {
  final policyPath =
      _parseStringArgument(arguments, '--policy=') ??
      'benchmarks/policies/ci_smoke.json';
  final selectedDatabases = _parseRequestedDatabases(arguments);
  final policy = await FirebirdBenchmarkGatePolicy.load(policyPath);
  final allTargets = policy.resolveTargets();
  final targets = _filterTargets(allTargets, selectedDatabases);
  final options = FirebirdBenchmarkOptions(
    warmupIterations:
        _parseIntArgument(arguments, '--warmup=') ??
        policy.options.warmupIterations,
    measuredIterations:
        _parseIntArgument(arguments, '--iterations=') ??
        policy.options.measuredIterations,
    statementTimeout:
        _parseDurationMillisecondsArgument(arguments, '--timeout-ms=') ??
        policy.options.statementTimeout,
  );

  final runner = FirebirdBenchmarkGateRunner(connect: _connectTarget);
  final summary = await runner.runTargets(targets, options: options);
  final jsonOutput = arguments.contains('--json');
  final payload = <String, Object?>{
    'policy': policy.toJson(),
    'selectedTargets': targets.map((target) => target.name).toList(),
    'summary': summary.toJson(),
  };

  if (jsonOutput) {
    print(const JsonEncoder.withIndent('  ').convert(payload));
  } else {
    print('# Firebird Benchmark CI Gate');
    print('');
    print('- Policy: `${policy.name}`');
    print('- Description: ${policy.description}');
    print('- Targets: `${targets.map((target) => target.name).join(', ')}`');
    print('- Warmup iterations: `${options.warmupIterations}`');
    print('- Measured iterations: `${options.measuredIterations}`');
    print(
      '- Statement timeout: `${options.statementTimeout?.inMilliseconds ?? 0} ms`',
    );
    print('');
    print('| Database | Snapshot | Failures | Verdict |');
    print('| --- | --- | ---: | --- |');
    for (final result in summary.results) {
      print(
        '| ${result.target.name} | ${result.target.snapshotPath} | '
        '${result.failingScenarioCount} | ${result.passed ? 'pass' : 'fail'} |',
      );
    }
    print('');
    print(
      '- Gate passed: `${summary.passed}` across `${summary.results.length}` target(s).',
    );
  }

  if (!summary.passed) {
    exitCode = 2;
  }
}

List<FirebirdBenchmarkTarget> _filterTargets(
  List<FirebirdBenchmarkTarget> targets,
  Set<String>? selectedDatabases,
) {
  if (selectedDatabases == null) return targets;

  final filtered = targets
      .where((target) => selectedDatabases.contains(target.name))
      .toList(growable: false);
  if (filtered.isEmpty) {
    throw ArgumentError(
      'The selected --database filter did not match any targets in the policy.',
    );
  }
  return filtered;
}

Future<FirebirdConnection> _connectTarget(FirebirdBenchmarkTarget target) {
  return FirebirdEndpoint(
    client: FirebirdFbClientNativeClient(
      fbClientLibraryPath:
          Platform.environment['FIREBIRDPOD_FBCLIENT_LIB'] ??
          '/Library/Frameworks/Firebird.framework/Versions/A/Resources/lib/libfbclient.dylib',
    ),
    options: FirebirdConnectionOptions(
      host: Platform.environment['FIREBIRDPOD_TEST_HOST'] ?? 'localhost',
      port: _parseInt(Platform.environment['FIREBIRDPOD_TEST_PORT']),
      database: target.databasePath,
      user: Platform.environment['FIREBIRDPOD_TEST_USER'] ?? 'sysdba',
      password:
          Platform.environment['FIREBIRDPOD_TEST_PASSWORD'] ?? 'masterkey',
    ),
  ).connect();
}

Set<String>? _parseRequestedDatabases(List<String> arguments) {
  final requested = <String>{};

  for (final argument in arguments) {
    if (!argument.startsWith('--database=')) continue;
    final value = argument.substring('--database='.length).trim();
    if (value.isEmpty || value == 'all') return null;
    requested.addAll(
      value
          .split(',')
          .map((item) => item.trim())
          .where((item) => item.isNotEmpty),
    );
  }

  return requested.isEmpty ? null : requested;
}

String? _parseStringArgument(List<String> arguments, String prefix) {
  for (final argument in arguments) {
    if (!argument.startsWith(prefix)) continue;
    final value = argument.substring(prefix.length).trim();
    return value.isEmpty ? null : value;
  }
  return null;
}

int? _parseIntArgument(List<String> arguments, String prefix) {
  for (final argument in arguments) {
    if (!argument.startsWith(prefix)) continue;
    return int.parse(argument.substring(prefix.length).trim());
  }
  return null;
}

Duration? _parseDurationMillisecondsArgument(
  List<String> arguments,
  String prefix,
) {
  final value = _parseIntArgument(arguments, prefix);
  return value == null || value <= 0 ? null : Duration(milliseconds: value);
}

int? _parseInt(String? value) {
  if (value == null || value.isEmpty) return null;
  return int.parse(value);
}
