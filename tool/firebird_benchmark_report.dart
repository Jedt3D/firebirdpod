import 'dart:convert';
import 'dart:io';

import 'package:firebirdpod/firebirdpod.dart';

Future<void> main(List<String> arguments) async {
  final selectedDatabases = _parseRequestedDatabases(arguments);
  final writeSnapshotPath = _parseStringArgument(
    arguments,
    '--write-snapshot=',
  );
  final compareSnapshotPath = _parseStringArgument(
    arguments,
    '--compare-snapshot=',
  );
  final options = FirebirdBenchmarkOptions(
    warmupIterations: _parseIntArgument(arguments, '--warmup=') ?? 1,
    measuredIterations: _parseIntArgument(arguments, '--iterations=') ?? 5,
    statementTimeout: _parseDurationMillisecondsArgument(
      arguments,
      '--timeout-ms=',
    ),
  );

  final targets = _availableBenchmarkTargets()
      .where(
        (target) =>
            selectedDatabases == null ||
            selectedDatabases.contains(target.name),
      )
      .toList(growable: false);

  if (targets.isEmpty) {
    throw ArgumentError(
      'No benchmarkable databases selected. Use --database=employee,chinook,northwind or --database=all.',
    );
  }

  if (writeSnapshotPath != null && targets.length != 1) {
    throw ArgumentError(
      '--write-snapshot requires exactly one selected benchmark database.',
    );
  }

  if (compareSnapshotPath != null && targets.length != 1) {
    throw ArgumentError(
      '--compare-snapshot requires exactly one selected benchmark database.',
    );
  }

  final defaultBudget = FirebirdBenchmarkBudget(
    maxMedianRegressionRatio:
        _parseDoubleArgument(arguments, '--median-budget=') ?? 1.20,
    maxP90RegressionRatio:
        _parseDoubleArgument(arguments, '--p90-budget=') ?? 1.25,
    minMedianRegressionDelta:
        _parseDurationMillisecondsArgument(arguments, '--median-floor-ms=') ??
        const Duration(milliseconds: 5),
    minP90RegressionDelta:
        _parseDurationMillisecondsArgument(arguments, '--p90-floor-ms=') ??
        const Duration(milliseconds: 10),
    failOnPlanChange: arguments.contains('--fail-on-plan-change'),
  );

  print('# Firebird Benchmark Report');
  print('');
  print('- Warmup iterations: `${options.warmupIterations}`');
  print('- Measured iterations: `${options.measuredIterations}`');
  print(
    '- Statement timeout: `${options.statementTimeout?.inMilliseconds ?? 0} ms`',
  );

  for (final target in targets) {
    final endpoint = _buildEndpoint(target.path);
    final connection = await endpoint.connect();
    try {
      final scenarios = firebirdDefaultBenchmarkScenariosForDatabase(
        target.name,
      );
      final suite = await connection.benchmarks.runScenarios(
        scenarios,
        options: options,
        databaseLabel: target.name,
      );
      FirebirdBenchmarkComparison? comparison;

      print('');
      print('## ${target.name}');
      print('');
      print(
        '| Scenario | Rows | Min ms | Median ms | P90 ms | Mean ms | Max ms | Plan lines |',
      );
      print('| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |');
      for (final result in suite.results) {
        print(
          '| ${result.scenario.name} | ${result.rowCount} | '
          '${_formatMilliseconds(result.statistics.minimum)} | '
          '${_formatMilliseconds(result.statistics.median)} | '
          '${_formatMilliseconds(result.statistics.p90)} | '
          '${_formatMilliseconds(result.statistics.mean)} | '
          '${_formatMilliseconds(result.statistics.maximum)} | '
          '${result.plan.lines.length} |',
        );
      }

      for (final result in suite.results) {
        print('');
        print('### ${result.scenario.name}');
        print('');
        print('- Description: ${result.scenario.description}');
        print('- Tags: `${result.scenario.tags.join(', ')}`');
        print('- Returned rows: `${result.rowCount}`');
        print('- Columns: `${result.columns.join(', ')}`');
        print(
          '- Total measured time: `${_formatMilliseconds(result.statistics.total)} ms`',
        );
        print('- Detailed plan:');
        for (final line in result.plan.lines) {
          print('  - $line');
        }
      }

      if (writeSnapshotPath case final snapshotPath?) {
        final snapshot = FirebirdBenchmarkSnapshot.fromSuiteResult(
          suite,
          defaultBudget: defaultBudget,
        );
        final snapshotFile = File(snapshotPath);
        await snapshotFile.parent.create(recursive: true);
        await snapshotFile.writeAsString(
          const JsonEncoder.withIndent('  ').convert(snapshot.toJson()),
        );
        print('');
        print('- Wrote snapshot: `${snapshotFile.path}`');
      }

      if (compareSnapshotPath case final snapshotPath?) {
        final snapshotFile = File(snapshotPath);
        final snapshot = FirebirdBenchmarkSnapshot.fromJson(
          (jsonDecode(await snapshotFile.readAsString())
                  as Map<Object?, Object?>)
              .cast<String, Object?>(),
        );
        comparison = snapshot.compare(suite);
        final comparisonBudget = comparison.snapshot.defaultBudget;

        print('');
        print('### Snapshot Comparison');
        print('');
        print(
          '| Scenario | Median delta ms | Median ratio | P90 delta ms | P90 ratio | Rows | Columns | Plan | Verdict |',
        );
        print('| --- | ---: | ---: | ---: | ---: | --- | --- | --- | --- |');
        for (final scenario in comparison.scenarios) {
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
        print('');
        print(
          '- Budget: median <= `${comparisonBudget.maxMedianRegressionRatio.toStringAsFixed(2)}x`'
          ' unless drift stays below `${comparisonBudget.minMedianRegressionDelta.inMilliseconds} ms`; '
          'p90 <= `${comparisonBudget.maxP90RegressionRatio.toStringAsFixed(2)}x`'
          ' unless drift stays below `${comparisonBudget.minP90RegressionDelta.inMilliseconds} ms`.',
        );
        print(
          '- Comparison passed: `${comparison.passed}`'
          '${comparison.databaseMatches ? '' : ' (database label mismatch)'}',
        );
      }

      if (comparison != null && !comparison.passed) {
        exitCode = 2;
      }
    } finally {
      await connection.close();
    }
  }
}

FirebirdEndpoint _buildEndpoint(String databasePath) {
  return FirebirdEndpoint(
    client: FirebirdFbClientNativeClient(
      fbClientLibraryPath:
          Platform.environment['FIREBIRDPOD_FBCLIENT_LIB'] ??
          '/Library/Frameworks/Firebird.framework/Versions/A/Resources/lib/libfbclient.dylib',
    ),
    options: FirebirdConnectionOptions(
      host: Platform.environment['FIREBIRDPOD_TEST_HOST'] ?? 'localhost',
      port: _parseInt(Platform.environment['FIREBIRDPOD_TEST_PORT']),
      database: databasePath,
      user: Platform.environment['FIREBIRDPOD_TEST_USER'] ?? 'sysdba',
      password:
          Platform.environment['FIREBIRDPOD_TEST_PASSWORD'] ?? 'masterkey',
    ),
  );
}

List<_BenchmarkTarget> _availableBenchmarkTargets() {
  return <_BenchmarkTarget>[
    _BenchmarkTarget(name: 'employee', path: _employeeDatabasePath()),
    _BenchmarkTarget(name: 'chinook', path: _chinookDatabasePath()),
    _BenchmarkTarget(name: 'northwind', path: _northwindDatabasePath()),
  ];
}

String _employeeDatabasePath() {
  return Platform.environment['FIREBIRDPOD_TEST_DATABASE'] ??
      '/Users/worajedt/GitHub/FireDart/databases/firebird/employee.fdb';
}

String _chinookDatabasePath() {
  return Platform.environment['FIREBIRDPOD_CHINOOK_DATABASE'] ??
      '/Users/worajedt/GitHub/FireDart/databases/firebird/chinook.fdb';
}

String _northwindDatabasePath() {
  return Platform.environment['FIREBIRDPOD_NORTHWIND_DATABASE'] ??
      '/Users/worajedt/GitHub/FireDart/databases/firebird/northwind.fdb';
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

double? _parseDoubleArgument(List<String> arguments, String prefix) {
  for (final argument in arguments) {
    if (!argument.startsWith(prefix)) continue;
    return double.parse(argument.substring(prefix.length).trim());
  }
  return null;
}

String? _parseStringArgument(List<String> arguments, String prefix) {
  for (final argument in arguments) {
    if (!argument.startsWith(prefix)) continue;
    final value = argument.substring(prefix.length).trim();
    return value.isEmpty ? null : value;
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

class _BenchmarkTarget {
  const _BenchmarkTarget({required this.name, required this.path});

  final String name;
  final String path;
}
