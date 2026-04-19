import 'dart:io';

import 'package:firebirdpod/firebirdpod.dart';

Future<void> main(List<String> arguments) async {
  final selectedDatabases = _parseRequestedDatabases(arguments);
  final options = FirebirdBenchmarkOptions(
    warmupIterations: _parseIntArgument(arguments, '--warmup=') ?? 1,
    measuredIterations: _parseIntArgument(arguments, '--iterations=') ?? 5,
    statementTimeout: _parseDurationMillisecondsArgument(
      arguments,
      '--timeout-ms=',
    ),
  );

  final targets = firebirdResolveBenchmarkTargets(selectedDatabases);
  final runner = FirebirdBenchmarkGateRunner(connect: _connectTarget);
  final summary = await runner.runTargets(targets, options: options);

  print('# Firebird Benchmark Gate');
  print('');
  print('- Databases: `${targets.map((target) => target.name).join(', ')}`');
  print('- Warmup iterations: `${options.warmupIterations}`');
  print('- Measured iterations: `${options.measuredIterations}`');
  print(
    '- Statement timeout: `${options.statementTimeout?.inMilliseconds ?? 0} ms`',
  );
  print('');
  print('| Database | Snapshot | Scenarios | Failures | Verdict |');
  print('| --- | --- | ---: | ---: | --- |');

  for (final result in summary.results) {
    print(
      '| ${result.target.name} | ${result.target.snapshotPath} | '
      '${result.comparison.scenarios.length} | ${result.failingScenarioCount} | '
      '${result.passed ? 'pass' : 'fail'} |',
    );

    if (result.passed) continue;

    print('');
    print('## ${result.target.name}');
    print('');
    print(
      '| Scenario | Median delta ms | Median ratio | P90 delta ms | P90 ratio | Rows | Columns | Plan | Verdict |',
    );
    print('| --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |');
    for (final scenario in result.comparison.scenarios) {
      print(
        '| ${scenario.name} | '
        '${_formatMillisecondsOrDash(scenario.medianDelta)} | '
        '${_formatRatio(scenario.medianRatio)} | '
        '${_formatMillisecondsOrDash(scenario.p90Delta)} | '
        '${_formatRatio(scenario.p90Ratio)} | '
        '${scenario.rowCountMatches ? 'match' : 'changed'} | '
        '${scenario.columnsMatch ? 'match' : 'changed'} | '
        '${scenario.planMatches ? 'match' : 'changed'} | '
        '${scenario.verdict} |',
      );
    }
  }

  print('');
  print(
    '- Gate passed: `${summary.passed}` across `${summary.results.length}` database target(s) and `${summary.failingScenarioCount}` failing scenario(s).',
  );

  if (!summary.passed) {
    exitCode = 2;
  }
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

String _formatMilliseconds(Duration duration) {
  return (duration.inMicroseconds / 1000).toStringAsFixed(3);
}

String _formatMillisecondsOrDash(Duration? duration) {
  if (duration == null) return '-';
  return _formatMilliseconds(duration);
}

String _formatRatio(double? ratio) {
  if (ratio == null) return '-';
  return ratio.toStringAsFixed(3);
}
