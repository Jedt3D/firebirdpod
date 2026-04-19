import 'dart:io';

import 'package:meta/meta.dart';

import 'firebird_benchmark_snapshots.dart';

const firebirdSupportedBenchmarkDatabases = <String>[
  'employee',
  'chinook',
  'northwind',
];

/// Database and snapshot locations for one supported benchmark target.
@immutable
class FirebirdBenchmarkTarget {
  const FirebirdBenchmarkTarget({
    required this.name,
    required this.databasePath,
    required this.snapshotPath,
    this.budgetOverride,
    this.scenarioNames,
  });

  final String name;
  final String databasePath;
  final String snapshotPath;
  final FirebirdBenchmarkBudget? budgetOverride;
  final List<String>? scenarioNames;
}

/// Returns the supported benchmark targets using the current environment.
List<FirebirdBenchmarkTarget> firebirdDefaultBenchmarkTargets({
  Map<String, String>? environment,
}) {
  final env = environment ?? Platform.environment;
  return <FirebirdBenchmarkTarget>[
    FirebirdBenchmarkTarget(
      name: 'employee',
      databasePath:
          env['FIREBIRDPOD_TEST_DATABASE'] ??
          '/Users/worajedt/GitHub/FireDart/databases/firebird/employee.fdb',
      snapshotPath:
          'benchmarks${Platform.pathSeparator}baselines${Platform.pathSeparator}employee.json',
    ),
    FirebirdBenchmarkTarget(
      name: 'chinook',
      databasePath:
          env['FIREBIRDPOD_CHINOOK_DATABASE'] ??
          '/Users/worajedt/GitHub/FireDart/databases/firebird/chinook.fdb',
      snapshotPath:
          'benchmarks${Platform.pathSeparator}baselines${Platform.pathSeparator}converted${Platform.pathSeparator}chinook.json',
    ),
    FirebirdBenchmarkTarget(
      name: 'northwind',
      databasePath:
          env['FIREBIRDPOD_NORTHWIND_DATABASE'] ??
          '/Users/worajedt/GitHub/FireDart/databases/firebird/northwind.fdb',
      snapshotPath:
          'benchmarks${Platform.pathSeparator}baselines${Platform.pathSeparator}converted${Platform.pathSeparator}northwind.json',
    ),
  ];
}

/// Resolves the selected benchmark targets while preserving the default order.
List<FirebirdBenchmarkTarget> firebirdResolveBenchmarkTargets(
  Iterable<String>? selected, {
  Map<String, String>? environment,
}) {
  final targets = firebirdDefaultBenchmarkTargets(environment: environment);
  if (selected == null) return targets;

  final requested = selected
      .map((item) => item.trim())
      .where((item) => item.isNotEmpty)
      .toSet();
  if (requested.isEmpty) return targets;

  final unknown =
      requested
          .where((name) => !firebirdSupportedBenchmarkDatabases.contains(name))
          .toList(growable: false)
        ..sort();
  if (unknown.isNotEmpty) {
    throw ArgumentError(
      'Unknown benchmark databases: ${unknown.join(', ')}. '
      'Use ${firebirdSupportedBenchmarkDatabases.join(', ')}, or all.',
    );
  }

  return targets
      .where((target) => requested.contains(target.name))
      .toList(growable: false);
}
