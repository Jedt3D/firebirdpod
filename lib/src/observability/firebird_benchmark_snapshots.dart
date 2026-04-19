import 'package:meta/meta.dart';

import 'firebird_benchmarks.dart';

/// Budget policy for comparing a benchmark run to a recorded snapshot.
@immutable
class FirebirdBenchmarkBudget {
  const FirebirdBenchmarkBudget({
    this.maxMedianRegressionRatio = 1.20,
    this.maxP90RegressionRatio = 1.25,
    this.minMedianRegressionDelta = const Duration(milliseconds: 5),
    this.minP90RegressionDelta = const Duration(milliseconds: 10),
    this.failOnPlanChange = false,
  }) : assert(
         maxMedianRegressionRatio >= 1.0,
         'maxMedianRegressionRatio must be at least 1.0.',
       ),
       assert(
         maxP90RegressionRatio >= 1.0,
         'maxP90RegressionRatio must be at least 1.0.',
       );

  final double maxMedianRegressionRatio;
  final double maxP90RegressionRatio;
  final Duration minMedianRegressionDelta;
  final Duration minP90RegressionDelta;
  final bool failOnPlanChange;

  factory FirebirdBenchmarkBudget.fromJson(Map<String, Object?> json) {
    return FirebirdBenchmarkBudget(
      maxMedianRegressionRatio:
          (json['maxMedianRegressionRatio'] as num?)?.toDouble() ?? 1.20,
      maxP90RegressionRatio:
          (json['maxP90RegressionRatio'] as num?)?.toDouble() ?? 1.25,
      minMedianRegressionDelta: _durationFromMicroseconds(
            json['minMedianRegressionDeltaMicroseconds'] as int?,
          ) ??
          const Duration(milliseconds: 5),
      minP90RegressionDelta:
          _durationFromMicroseconds(
            json['minP90RegressionDeltaMicroseconds'] as int?,
          ) ??
          const Duration(milliseconds: 10),
      failOnPlanChange: json['failOnPlanChange'] as bool? ?? false,
    );
  }

  Map<String, Object?> toJson() {
    return <String, Object?>{
      'maxMedianRegressionRatio': maxMedianRegressionRatio,
      'maxP90RegressionRatio': maxP90RegressionRatio,
      'minMedianRegressionDeltaMicroseconds':
          minMedianRegressionDelta.inMicroseconds,
      'minP90RegressionDeltaMicroseconds':
          minP90RegressionDelta.inMicroseconds,
      'failOnPlanChange': failOnPlanChange,
    };
  }
}

/// A recorded benchmark baseline for one database.
@immutable
class FirebirdBenchmarkSnapshot {
  const FirebirdBenchmarkSnapshot({
    required this.formatVersion,
    required this.databaseLabel,
    required this.recordedAt,
    required this.options,
    required this.defaultBudget,
    required this.scenarios,
  });

  factory FirebirdBenchmarkSnapshot.fromSuiteResult(
    FirebirdBenchmarkSuiteResult suite, {
    FirebirdBenchmarkBudget defaultBudget = const FirebirdBenchmarkBudget(),
  }) {
    return FirebirdBenchmarkSnapshot(
      formatVersion: 1,
      databaseLabel: suite.databaseLabel,
      recordedAt: suite.finishedAt,
      options: suite.options,
      defaultBudget: defaultBudget,
      scenarios: suite.results
          .map(
            (result) => FirebirdBenchmarkScenarioSnapshot.fromScenarioResult(
              result,
              budget: defaultBudget,
            ),
          )
          .toList(growable: false),
    );
  }

  factory FirebirdBenchmarkSnapshot.fromJson(Map<String, Object?> json) {
    final rawScenarios =
        json['scenarios'] as List<Object?>? ?? const <Object?>[];
    return FirebirdBenchmarkSnapshot(
      formatVersion: json['formatVersion'] as int? ?? 1,
      databaseLabel:
          json['databaseLabel'] as String? ??
          (throw StateError('Benchmark snapshot is missing databaseLabel.')),
      recordedAt:
          DateTime.tryParse(json['recordedAt'] as String? ?? '') ??
          (throw StateError('Benchmark snapshot has an invalid recordedAt.')),
      options: FirebirdBenchmarkOptions(
        warmupIterations: json['warmupIterations'] as int? ?? 1,
        measuredIterations: json['measuredIterations'] as int? ?? 1,
        statementTimeout: _durationFromMicroseconds(
          json['statementTimeoutMicroseconds'] as int?,
        ),
      ),
      defaultBudget: FirebirdBenchmarkBudget.fromJson(
        (json['defaultBudget'] as Map<Object?, Object?>? ?? const {})
            .cast<String, Object?>(),
      ),
      scenarios: rawScenarios
          .map(
            (rawScenario) => FirebirdBenchmarkScenarioSnapshot.fromJson(
              (rawScenario as Map<Object?, Object?>).cast<String, Object?>(),
            ),
          )
          .toList(growable: false),
    );
  }

  final int formatVersion;
  final String databaseLabel;
  final DateTime recordedAt;
  final FirebirdBenchmarkOptions options;
  final FirebirdBenchmarkBudget defaultBudget;
  final List<FirebirdBenchmarkScenarioSnapshot> scenarios;

  Map<String, Object?> toJson() {
    return <String, Object?>{
      'formatVersion': formatVersion,
      'databaseLabel': databaseLabel,
      'recordedAt': recordedAt.toIso8601String(),
      'warmupIterations': options.warmupIterations,
      'measuredIterations': options.measuredIterations,
      'statementTimeoutMicroseconds':
          options.statementTimeout?.inMicroseconds ?? 0,
      'defaultBudget': defaultBudget.toJson(),
      'scenarios': scenarios.map((scenario) => scenario.toJson()).toList(),
    };
  }

  FirebirdBenchmarkComparison compare(FirebirdBenchmarkSuiteResult suite) {
    final currentByName = <String, FirebirdBenchmarkScenarioResult>{
      for (final result in suite.results) result.scenario.name: result,
    };
    final snapshotByName = <String, FirebirdBenchmarkScenarioSnapshot>{
      for (final scenario in scenarios) scenario.name: scenario,
    };
    final allNames = <String>{
      ...snapshotByName.keys,
      ...currentByName.keys,
    }.toList()..sort();

    final comparisons = allNames
        .map(
          (name) => FirebirdBenchmarkScenarioComparison(
            name: name,
            snapshotScenario: snapshotByName[name],
            currentResult: currentByName[name],
            defaultBudget: defaultBudget,
          ),
        )
        .toList(growable: false);

    return FirebirdBenchmarkComparison(
      snapshot: this,
      suite: suite,
      databaseMatches: databaseLabel == suite.databaseLabel,
      scenarios: comparisons,
    );
  }
}

/// One recorded scenario inside a benchmark snapshot.
@immutable
class FirebirdBenchmarkScenarioSnapshot {
  const FirebirdBenchmarkScenarioSnapshot({
    required this.database,
    required this.name,
    required this.description,
    required this.tags,
    required this.rowCount,
    required this.columns,
    required this.median,
    required this.p90,
    required this.mean,
    required this.minimum,
    required this.maximum,
    required this.plan,
    required this.budget,
  });

  factory FirebirdBenchmarkScenarioSnapshot.fromScenarioResult(
    FirebirdBenchmarkScenarioResult result, {
    required FirebirdBenchmarkBudget budget,
  }) {
    return FirebirdBenchmarkScenarioSnapshot(
      database: result.scenario.database,
      name: result.scenario.name,
      description: result.scenario.description,
      tags: List<String>.unmodifiable(result.scenario.tags),
      rowCount: result.rowCount,
      columns: List<String>.unmodifiable(result.columns),
      median: result.statistics.median,
      p90: result.statistics.p90,
      mean: result.statistics.mean,
      minimum: result.statistics.minimum,
      maximum: result.statistics.maximum,
      plan: result.plan.plan,
      budget: budget,
    );
  }

  factory FirebirdBenchmarkScenarioSnapshot.fromJson(
    Map<String, Object?> json,
  ) {
    return FirebirdBenchmarkScenarioSnapshot(
      database:
          json['database'] as String? ??
          (throw StateError('Snapshot scenario is missing database.')),
      name:
          json['name'] as String? ??
          (throw StateError('Snapshot scenario is missing name.')),
      description: json['description'] as String? ?? '',
      tags: (json['tags'] as List<Object?>? ?? const <Object?>[])
          .map((tag) => tag as String)
          .toList(growable: false),
      rowCount: json['rowCount'] as int? ?? 0,
      columns: (json['columns'] as List<Object?>? ?? const <Object?>[])
          .map((column) => column as String)
          .toList(growable: false),
      median: _durationFromMicroseconds(json['medianMicroseconds'] as int?)!,
      p90: _durationFromMicroseconds(json['p90Microseconds'] as int?)!,
      mean: _durationFromMicroseconds(json['meanMicroseconds'] as int?)!,
      minimum: _durationFromMicroseconds(json['minimumMicroseconds'] as int?)!,
      maximum: _durationFromMicroseconds(json['maximumMicroseconds'] as int?)!,
      plan: json['plan'] as String? ?? '',
      budget: FirebirdBenchmarkBudget.fromJson(
        (json['budget'] as Map<Object?, Object?>? ?? const {})
            .cast<String, Object?>(),
      ),
    );
  }

  final String database;
  final String name;
  final String description;
  final List<String> tags;
  final int rowCount;
  final List<String> columns;
  final Duration median;
  final Duration p90;
  final Duration mean;
  final Duration minimum;
  final Duration maximum;
  final String plan;
  final FirebirdBenchmarkBudget budget;

  Map<String, Object?> toJson() {
    return <String, Object?>{
      'database': database,
      'name': name,
      'description': description,
      'tags': tags,
      'rowCount': rowCount,
      'columns': columns,
      'medianMicroseconds': median.inMicroseconds,
      'p90Microseconds': p90.inMicroseconds,
      'meanMicroseconds': mean.inMicroseconds,
      'minimumMicroseconds': minimum.inMicroseconds,
      'maximumMicroseconds': maximum.inMicroseconds,
      'plan': plan,
      'budget': budget.toJson(),
    };
  }
}

/// Comparison result between a live benchmark suite and a stored snapshot.
@immutable
class FirebirdBenchmarkComparison {
  const FirebirdBenchmarkComparison({
    required this.snapshot,
    required this.suite,
    required this.databaseMatches,
    required this.scenarios,
  });

  final FirebirdBenchmarkSnapshot snapshot;
  final FirebirdBenchmarkSuiteResult suite;
  final bool databaseMatches;
  final List<FirebirdBenchmarkScenarioComparison> scenarios;

  bool get passed =>
      databaseMatches && scenarios.every((comparison) => comparison.passed);
}

/// Comparison result for one scenario.
@immutable
class FirebirdBenchmarkScenarioComparison {
  FirebirdBenchmarkScenarioComparison({
    required this.name,
    required this.snapshotScenario,
    required this.currentResult,
    required FirebirdBenchmarkBudget defaultBudget,
  }) : budget = snapshotScenario?.budget ?? defaultBudget;

  final String name;
  final FirebirdBenchmarkScenarioSnapshot? snapshotScenario;
  final FirebirdBenchmarkScenarioResult? currentResult;
  final FirebirdBenchmarkBudget budget;

  bool get missingInCurrent =>
      snapshotScenario != null && currentResult == null;

  bool get unexpectedInCurrent =>
      snapshotScenario == null && currentResult != null;

  bool get rowCountMatches =>
      snapshotScenario != null &&
      currentResult != null &&
      snapshotScenario!.rowCount == currentResult!.rowCount;

  bool get columnsMatch =>
      snapshotScenario != null &&
      currentResult != null &&
      _listEquals(snapshotScenario!.columns, currentResult!.columns);

  bool get planMatches =>
      snapshotScenario != null &&
      currentResult != null &&
      snapshotScenario!.plan == currentResult!.plan.plan;

  double? get medianRatio =>
      _ratio(snapshotScenario?.median, currentResult?.statistics.median);

  double? get p90Ratio =>
      _ratio(snapshotScenario?.p90, currentResult?.statistics.p90);

  bool get medianRegressed =>
      medianRatio != null &&
      medianRatio! > budget.maxMedianRegressionRatio &&
      medianDelta != null &&
      medianDelta! >= budget.minMedianRegressionDelta;

  bool get p90Regressed =>
      p90Ratio != null &&
      p90Ratio! > budget.maxP90RegressionRatio &&
      p90Delta != null &&
      p90Delta! >= budget.minP90RegressionDelta;

  Duration? get medianDelta =>
      _delta(snapshotScenario?.median, currentResult?.statistics.median);

  Duration? get p90Delta =>
      _delta(snapshotScenario?.p90, currentResult?.statistics.p90);

  bool get passed {
    if (missingInCurrent || unexpectedInCurrent) return false;
    if (!rowCountMatches || !columnsMatch) return false;
    if (medianRegressed || p90Regressed) return false;
    if (budget.failOnPlanChange && !planMatches) return false;
    return true;
  }

  String get verdict {
    if (missingInCurrent) return 'missing';
    if (unexpectedInCurrent) return 'unexpected';
    if (!rowCountMatches) return 'row-count-changed';
    if (!columnsMatch) return 'columns-changed';
    if (medianRegressed || p90Regressed) return 'regressed';
    if (budget.failOnPlanChange && !planMatches) return 'plan-changed';
    if (!planMatches) return 'pass-with-plan-drift';
    return 'pass';
  }
}

Duration? _durationFromMicroseconds(int? value) {
  if (value == null || value == 0) return null;
  return Duration(microseconds: value);
}

double? _ratio(Duration? baseline, Duration? current) {
  if (baseline == null || current == null || baseline.inMicroseconds == 0) {
    return null;
  }

  return current.inMicroseconds / baseline.inMicroseconds;
}

Duration? _delta(Duration? baseline, Duration? current) {
  if (baseline == null || current == null) return null;
  return current - baseline;
}

bool _listEquals(List<String> left, List<String> right) {
  if (identical(left, right)) return true;
  if (left.length != right.length) return false;

  for (var index = 0; index < left.length; index++) {
    if (left[index] != right[index]) return false;
  }
  return true;
}
