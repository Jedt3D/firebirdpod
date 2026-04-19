import 'dart:io';

import 'package:firebirdpod/firebirdpod.dart';

Future<void> main(List<String> arguments) async {
  final selectedDatabases = _parseRequestedDatabases(arguments);
  final showPassing = arguments.contains('--show-passing');
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
  final advisor = FirebirdBenchmarkTuningAdvisor();

  print('# Firebird Benchmark Tuning Report');
  print('');
  print('- Databases: `${targets.map((target) => target.name).join(', ')}`');
  print('- Warmup iterations: `${options.warmupIterations}`');
  print('- Measured iterations: `${options.measuredIterations}`');
  print(
    '- Statement timeout: `${options.statementTimeout?.inMilliseconds ?? 0} ms`',
  );
  print('');

  for (final result in summary.results) {
    final report = advisor.analyzeGateResult(result);
    final scenarios = showPassing
        ? report.scenarios
        : report.interestingScenarios;

    print('## ${result.target.name}');
    print('');
    print('- Gate passed: `${result.passed}`');
    print('- Snapshot: `${result.target.snapshotPath}`');
    print('- Failing scenarios: `${result.failingScenarioCount}`');
    print('- Recommended shared next steps:');
    for (final step in report.sharedNextSteps) {
      print('  - $step');
    }

    if (scenarios.isEmpty) {
      print('');
      print('- No scenario-specific investigation steps are needed right now.');
      print('');
      continue;
    }

    for (final scenario in scenarios) {
      print('');
      print('### ${scenario.name}');
      print('');
      print('- Verdict: `${scenario.verdict}`');
      print('- Severity: `${scenario.severity.name}`');
      print('- Headline: ${scenario.headline}');
      print('- Summary: ${scenario.summary}');
      print(
        '- Median: `${_formatMillisecondsOrDash(scenario.medianBaseline)}` -> `${_formatMillisecondsOrDash(scenario.medianCurrent)}` ms'
        ' (delta `${_formatMillisecondsOrDash(scenario.medianDelta)}`, ratio `${_formatRatio(scenario.medianRatio)}`)',
      );
      print(
        '- P90: `${_formatMillisecondsOrDash(scenario.p90Baseline)}` -> `${_formatMillisecondsOrDash(scenario.p90Current)}` ms'
        ' (delta `${_formatMillisecondsOrDash(scenario.p90Delta)}`, ratio `${_formatRatio(scenario.p90Ratio)}`)',
      );
      print(
        '- Shape checks: rows `${scenario.rowCountMatches ? 'match' : 'changed'}`, '
        'columns `${scenario.columnsMatch ? 'match' : 'changed'}`, '
        'plan `${scenario.planMatches ? 'match' : 'changed'}`',
      );
      print('- Next steps:');
      for (final step in scenario.nextSteps) {
        print('  - $step');
      }
    }

    print('');
  }

  print(
    '- Report requires action: `${summary.passed ? false : true}` across `${summary.results.length}` database target(s).',
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
