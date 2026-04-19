import 'dart:convert';
import 'dart:io';

import 'package:meta/meta.dart';

import '../runtime/firebird_connection.dart';
import 'firebird_benchmark_catalog.dart';
import 'firebird_benchmark_snapshots.dart';
import 'firebird_benchmarks.dart';

typedef FirebirdBenchmarkConnectionFactory =
    Future<FirebirdConnection> Function(FirebirdBenchmarkTarget target);

/// Runs all selected benchmark targets against their recorded snapshots.
class FirebirdBenchmarkGateRunner {
  const FirebirdBenchmarkGateRunner({
    required FirebirdBenchmarkConnectionFactory connect,
  }) : _connect = connect;

  final FirebirdBenchmarkConnectionFactory _connect;

  Future<FirebirdBenchmarkGateSummary> runTargets(
    List<FirebirdBenchmarkTarget> targets, {
    FirebirdBenchmarkOptions options = const FirebirdBenchmarkOptions(),
  }) async {
    if (targets.isEmpty) {
      throw ArgumentError.value(
        targets,
        'targets',
        'At least one benchmark target is required.',
      );
    }

    final results = <FirebirdBenchmarkGateTargetResult>[];
    for (final target in targets) {
      var snapshot = await _loadSnapshot(target.snapshotPath);
      final scenarios = firebirdResolveBenchmarkScenariosForDatabase(
        target.name,
        selectedScenarioNames: target.scenarioNames,
      );
      if (target.scenarioNames case final selectedNames?) {
        snapshot = snapshot.selectScenarios(selectedNames);
      }
      if (target.budgetOverride case final budget?) {
        snapshot = snapshot.withBudget(budget);
      }
      final connection = await _connect(target);
      try {
        final suite = await connection.benchmarks.runScenarios(
          scenarios,
          options: options,
          databaseLabel: target.name,
        );
        final comparison = snapshot.compare(suite);
        results.add(
          FirebirdBenchmarkGateTargetResult(
            target: target,
            snapshot: snapshot,
            suite: suite,
            comparison: comparison,
          ),
        );
      } finally {
        await connection.close();
      }
    }

    return FirebirdBenchmarkGateSummary(results: results);
  }

  Future<FirebirdBenchmarkSnapshot> _loadSnapshot(String snapshotPath) async {
    final snapshotFile = File(snapshotPath);
    final rawJson =
        (jsonDecode(await snapshotFile.readAsString()) as Map<Object?, Object?>)
            .cast<String, Object?>();
    return FirebirdBenchmarkSnapshot.fromJson(rawJson);
  }
}

@immutable
class FirebirdBenchmarkGateTargetResult {
  const FirebirdBenchmarkGateTargetResult({
    required this.target,
    required this.snapshot,
    required this.suite,
    required this.comparison,
  });

  final FirebirdBenchmarkTarget target;
  final FirebirdBenchmarkSnapshot snapshot;
  final FirebirdBenchmarkSuiteResult suite;
  final FirebirdBenchmarkComparison comparison;

  bool get passed => comparison.passed;

  int get failingScenarioCount =>
      comparison.scenarios.where((scenario) => !scenario.passed).length;

  Map<String, Object?> toJson() {
    return <String, Object?>{
      'target': target.name,
      'databasePath': target.databasePath,
      'snapshotPath': target.snapshotPath,
      'scenarioNames': target.scenarioNames,
      'passed': passed,
      'failingScenarioCount': failingScenarioCount,
      'comparison': comparison.toJson(),
    };
  }
}

@immutable
class FirebirdBenchmarkGateSummary {
  const FirebirdBenchmarkGateSummary({required this.results});

  final List<FirebirdBenchmarkGateTargetResult> results;

  bool get passed => results.every((result) => result.passed);

  int get failedTargetCount => results.where((result) => !result.passed).length;

  int get failingScenarioCount => results.fold<int>(
    0,
    (total, result) => total + result.failingScenarioCount,
  );

  Map<String, Object?> toJson() {
    return <String, Object?>{
      'passed': passed,
      'failedTargetCount': failedTargetCount,
      'failingScenarioCount': failingScenarioCount,
      'results': results.map((result) => result.toJson()).toList(),
    };
  }
}
