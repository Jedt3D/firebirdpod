import 'dart:convert';
import 'dart:io';

import 'package:meta/meta.dart';

import 'firebird_benchmark_catalog.dart';
import 'firebird_benchmark_snapshots.dart';
import 'firebird_benchmarks.dart';

@immutable
class FirebirdBenchmarkGatePolicy {
  const FirebirdBenchmarkGatePolicy({
    required this.formatVersion,
    required this.name,
    required this.description,
    required this.options,
    required this.targets,
  });

  factory FirebirdBenchmarkGatePolicy.fromJson(Map<String, Object?> json) {
    final rawTargets = json['targets'] as List<Object?>? ?? const <Object?>[];
    return FirebirdBenchmarkGatePolicy(
      formatVersion: json['formatVersion'] as int? ?? 1,
      name:
          json['name'] as String? ??
          (throw StateError('Benchmark gate policy is missing name.')),
      description: json['description'] as String? ?? '',
      options: FirebirdBenchmarkOptions(
        warmupIterations: json['warmupIterations'] as int? ?? 1,
        measuredIterations: json['measuredIterations'] as int? ?? 5,
        statementTimeout: _durationFromMicroseconds(
          json['statementTimeoutMicroseconds'] as int?,
        ),
      ),
      targets: rawTargets
          .map(
            (rawTarget) => FirebirdBenchmarkGatePolicyTarget.fromJson(
              (rawTarget as Map<Object?, Object?>).cast<String, Object?>(),
            ),
          )
          .toList(growable: false),
    );
  }

  final int formatVersion;
  final String name;
  final String description;
  final FirebirdBenchmarkOptions options;
  final List<FirebirdBenchmarkGatePolicyTarget> targets;

  static Future<FirebirdBenchmarkGatePolicy> load(String path) async {
    final file = File(path);
    final rawJson =
        (jsonDecode(await file.readAsString()) as Map<Object?, Object?>)
            .cast<String, Object?>();
    return FirebirdBenchmarkGatePolicy.fromJson(rawJson);
  }

  Map<String, Object?> toJson() {
    return <String, Object?>{
      'formatVersion': formatVersion,
      'name': name,
      'description': description,
      'warmupIterations': options.warmupIterations,
      'measuredIterations': options.measuredIterations,
      'statementTimeoutMicroseconds': options.statementTimeout?.inMicroseconds,
      'targets': targets.map((target) => target.toJson()).toList(),
    };
  }

  List<FirebirdBenchmarkTarget> resolveTargets({
    Map<String, String>? environment,
  }) {
    final defaults = firebirdDefaultBenchmarkTargets(environment: environment);
    final defaultsByName = <String, FirebirdBenchmarkTarget>{
      for (final target in defaults) target.name: target,
    };

    return targets
        .map((policyTarget) {
          final defaultTarget = defaultsByName[policyTarget.name];
          if (defaultTarget == null) {
            throw StateError(
              'Benchmark gate policy references unknown target ${policyTarget.name}.',
            );
          }

          return FirebirdBenchmarkTarget(
            name: defaultTarget.name,
            databasePath: defaultTarget.databasePath,
            snapshotPath:
                policyTarget.snapshotPath ?? defaultTarget.snapshotPath,
            budgetOverride: policyTarget.budgetOverride,
            scenarioNames: policyTarget.scenarioNames,
          );
        })
        .toList(growable: false);
  }
}

Duration? _durationFromMicroseconds(int? value) {
  if (value == null || value == 0) return null;
  return Duration(microseconds: value);
}

@immutable
class FirebirdBenchmarkGatePolicyTarget {
  const FirebirdBenchmarkGatePolicyTarget({
    required this.name,
    this.snapshotPath,
    this.budgetOverride,
    this.scenarioNames,
  });

  factory FirebirdBenchmarkGatePolicyTarget.fromJson(
    Map<String, Object?> json,
  ) {
    final budgetJson = json['budgetOverride'] as Map<Object?, Object?>?;
    return FirebirdBenchmarkGatePolicyTarget(
      name:
          json['name'] as String? ??
          (throw StateError('Benchmark gate policy target is missing name.')),
      snapshotPath: json['snapshotPath'] as String?,
      budgetOverride: budgetJson == null
          ? null
          : FirebirdBenchmarkBudget.fromJson(
              budgetJson.cast<String, Object?>(),
            ),
      scenarioNames: (json['scenarioNames'] as List<Object?>?)
          ?.map((name) => name as String)
          .toList(growable: false),
    );
  }

  final String name;
  final String? snapshotPath;
  final FirebirdBenchmarkBudget? budgetOverride;
  final List<String>? scenarioNames;

  Map<String, Object?> toJson() {
    return <String, Object?>{
      'name': name,
      if (snapshotPath != null) 'snapshotPath': snapshotPath,
      if (budgetOverride != null) 'budgetOverride': budgetOverride!.toJson(),
      if (scenarioNames != null) 'scenarioNames': scenarioNames,
    };
  }
}
