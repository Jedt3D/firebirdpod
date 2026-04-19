import 'package:meta/meta.dart';

import '../runtime/firebird_connection.dart';
import '../runtime/firebird_execution_result.dart';
import '../sql/firebird_statement_parameters.dart';
import 'firebird_query_plans.dart';

/// Repeatable benchmark execution over a Firebird connection.
class FirebirdBenchmarkRunner {
  const FirebirdBenchmarkRunner(this._connection);

  final FirebirdConnection _connection;

  Future<FirebirdBenchmarkScenarioResult> runScenario(
    FirebirdBenchmarkScenario scenario, {
    FirebirdBenchmarkOptions options = const FirebirdBenchmarkOptions(),
  }) async {
    final parameters = scenario.parameters;
    final plan = await _connection.queryPlans.explain(scenario.query);

    for (var iteration = 0; iteration < options.warmupIterations; iteration++) {
      await _connection.execute(
        scenario.query,
        parameters: parameters,
        timeout: options.statementTimeout,
      );
    }

    final samples = <Duration>[];
    FirebirdExecutionResult? lastResult;

    for (
      var iteration = 0;
      iteration < options.measuredIterations;
      iteration++
    ) {
      final stopwatch = Stopwatch()..start();
      lastResult = await _connection.execute(
        scenario.query,
        parameters: parameters,
        timeout: options.statementTimeout,
      );
      stopwatch.stop();
      samples.add(stopwatch.elapsed);
    }

    final statistics = FirebirdBenchmarkStatistics.fromSamples(samples);
    return FirebirdBenchmarkScenarioResult(
      scenario: scenario,
      options: options,
      statistics: statistics,
      plan: plan,
      rowCount: lastResult?.rows.length ?? 0,
      affectedRows: lastResult?.affectedRows ?? 0,
      columns: lastResult?.rows.isEmpty ?? true
          ? const <String>[]
          : lastResult!.rows.first.keys.toList(growable: false),
    );
  }

  Future<FirebirdBenchmarkSuiteResult> runScenarios(
    List<FirebirdBenchmarkScenario> scenarios, {
    FirebirdBenchmarkOptions options = const FirebirdBenchmarkOptions(),
    String? databaseLabel,
  }) async {
    if (scenarios.isEmpty) {
      throw ArgumentError.value(
        scenarios,
        'scenarios',
        'At least one benchmark scenario is required.',
      );
    }

    final startedAt = DateTime.now();
    final results = <FirebirdBenchmarkScenarioResult>[];
    for (final scenario in scenarios) {
      results.add(await runScenario(scenario, options: options));
    }

    return FirebirdBenchmarkSuiteResult(
      databaseLabel: databaseLabel ?? scenarios.first.database,
      options: options,
      startedAt: startedAt,
      finishedAt: DateTime.now(),
      results: results,
    );
  }
}

extension FirebirdConnectionBenchmarks on FirebirdConnection {
  FirebirdBenchmarkRunner get benchmarks => FirebirdBenchmarkRunner(this);
}

@immutable
class FirebirdBenchmarkOptions {
  const FirebirdBenchmarkOptions({
    this.warmupIterations = 1,
    this.measuredIterations = 5,
    this.statementTimeout,
  }) : assert(warmupIterations >= 0, 'warmupIterations must be non-negative.'),
       assert(
         measuredIterations > 0,
         'measuredIterations must be greater than zero.',
       );

  final int warmupIterations;
  final int measuredIterations;
  final Duration? statementTimeout;
}

@immutable
class FirebirdBenchmarkScenario {
  const FirebirdBenchmarkScenario({
    required this.database,
    required this.name,
    required this.description,
    required this.query,
    this.namedParameters = const <String, Object?>{},
    this.positionalParameters = const <Object?>[],
    this.tags = const <String>[],
  });

  final String database;
  final String name;
  final String description;
  final String query;
  final Map<String, Object?> namedParameters;
  final List<Object?> positionalParameters;
  final List<String> tags;

  FirebirdStatementParameters? get parameters {
    if (namedParameters.isNotEmpty && positionalParameters.isNotEmpty) {
      throw StateError(
        'A benchmark scenario cannot define both named and positional parameters.',
      );
    }
    if (namedParameters.isNotEmpty) {
      return FirebirdStatementParameters.named(namedParameters);
    }
    if (positionalParameters.isNotEmpty) {
      return FirebirdStatementParameters.positional(positionalParameters);
    }
    return null;
  }
}

@immutable
class FirebirdBenchmarkScenarioResult {
  const FirebirdBenchmarkScenarioResult({
    required this.scenario,
    required this.options,
    required this.statistics,
    required this.plan,
    required this.rowCount,
    required this.affectedRows,
    required this.columns,
  });

  final FirebirdBenchmarkScenario scenario;
  final FirebirdBenchmarkOptions options;
  final FirebirdBenchmarkStatistics statistics;
  final FirebirdQueryPlan plan;
  final int rowCount;
  final int affectedRows;
  final List<String> columns;
}

@immutable
class FirebirdBenchmarkSuiteResult {
  const FirebirdBenchmarkSuiteResult({
    required this.databaseLabel,
    required this.options,
    required this.startedAt,
    required this.finishedAt,
    required this.results,
  });

  final String databaseLabel;
  final FirebirdBenchmarkOptions options;
  final DateTime startedAt;
  final DateTime finishedAt;
  final List<FirebirdBenchmarkScenarioResult> results;

  Duration get elapsed => finishedAt.difference(startedAt);
}

@immutable
class FirebirdBenchmarkStatistics {
  const FirebirdBenchmarkStatistics({
    required this.samples,
    required this.minimum,
    required this.maximum,
    required this.mean,
    required this.median,
    required this.p90,
    required this.total,
  });

  factory FirebirdBenchmarkStatistics.fromSamples(List<Duration> samples) {
    if (samples.isEmpty) {
      throw ArgumentError.value(
        samples,
        'samples',
        'Samples must not be empty.',
      );
    }

    final sortedMicroseconds =
        samples.map((sample) => sample.inMicroseconds).toList(growable: false)
          ..sort();
    final totalMicroseconds = sortedMicroseconds.fold<int>(
      0,
      (total, value) => total + value,
    );

    return FirebirdBenchmarkStatistics(
      samples: List<Duration>.unmodifiable(samples),
      minimum: Duration(microseconds: sortedMicroseconds.first),
      maximum: Duration(microseconds: sortedMicroseconds.last),
      mean: Duration(
        microseconds: totalMicroseconds ~/ sortedMicroseconds.length,
      ),
      median: Duration(microseconds: _medianMicroseconds(sortedMicroseconds)),
      p90: Duration(
        microseconds: _percentileMicroseconds(sortedMicroseconds, 0.90),
      ),
      total: Duration(microseconds: totalMicroseconds),
    );
  }

  final List<Duration> samples;
  final Duration minimum;
  final Duration maximum;
  final Duration mean;
  final Duration median;
  final Duration p90;
  final Duration total;
}

const firebirdChinookBenchmarkScenarios = <FirebirdBenchmarkScenario>[
  FirebirdBenchmarkScenario(
    database: 'chinook',
    name: 'track_catalog_join',
    description:
        'Catalog join from tracks through albums to artists over most of the track table.',
    query: '''
      select
        t."TrackId",
        t."Name",
        a."Title",
        ar."Name" as "ArtistName"
      from "tracks" t
      join "albums" a on a."AlbumId" = t."AlbumId"
      join "artists" ar on ar."ArtistId" = a."ArtistId"
      where t."TrackId" between @trackStart and @trackEnd
      order by t."TrackId"
      ''',
    namedParameters: <String, Object?>{'trackStart': 1, 'trackEnd': 3000},
    tags: <String>['join', 'catalog', 'parameterized'],
  ),
  FirebirdBenchmarkScenario(
    database: 'chinook',
    name: 'invoice_customer_rollup',
    description:
        'Grouped invoice-line revenue rollup by customer over the main sales catalog.',
    query: '''
      select
        c."CustomerId",
        count(*) as "InvoiceLineCount",
        sum(ii."Quantity") as "UnitsPurchased",
        sum(ii."UnitPrice" * ii."Quantity") as "Revenue"
      from "customers" c
      join "invoices" i on i."CustomerId" = c."CustomerId"
      join "invoice_items" ii on ii."InvoiceId" = i."InvoiceId"
      join "tracks" t on t."TrackId" = ii."TrackId"
      where t."TrackId" between @trackStart and @trackEnd
      group by c."CustomerId"
      order by 4 desc, 1
      ''',
    namedParameters: <String, Object?>{'trackStart': 1, 'trackEnd': 3000},
    tags: <String>['aggregate', 'group-by', 'parameterized'],
  ),
];

const firebirdNorthwindBenchmarkScenarios = <FirebirdBenchmarkScenario>[
  FirebirdBenchmarkScenario(
    database: 'northwind',
    name: 'order_rollup',
    description:
        'Order-detail revenue rollup over a representative order range.',
    query: '''
      select first 25
        o."CustomerID",
        o."OrderID",
        sum(od."UnitPrice" * od."Quantity" * (1 - od."Discount")) as "OrderTotal"
      from "Orders" o
      join "Order Details" od on od."OrderID" = o."OrderID"
      where o."OrderID" between @orderStart and @orderEnd
      group by o."CustomerID", o."OrderID"
      order by 3 desc, o."OrderID"
      ''',
    namedParameters: <String, Object?>{'orderStart': 10248, 'orderEnd': 11248},
    tags: <String>['aggregate', 'join', 'parameterized'],
  ),
  FirebirdBenchmarkScenario(
    database: 'northwind',
    name: 'product_sales_rollup',
    description:
        'Aggregated sales totals by product over the reporting fixture.',
    query: '''
      select first 20
        od."ProductID",
        sum(od."Quantity") as "UnitsSold",
        sum(od."Quantity" * od."UnitPrice") as "Revenue"
      from "Order Details" od
      join "Orders" o on o."OrderID" = od."OrderID"
      where od."ProductID" between @productStart and @productEnd
      group by od."ProductID"
      order by 3 desc, 1
      ''',
    namedParameters: <String, Object?>{'productStart': 1, 'productEnd': 50},
    tags: <String>['aggregate', 'reporting', 'parameterized'],
  ),
];

const firebirdEmployeeBenchmarkScenarios = <FirebirdBenchmarkScenario>[
  FirebirdBenchmarkScenario(
    database: 'employee',
    name: 'employee_directory_join',
    description:
        'Employee-directory join across employee, department, and job over the main staff range.',
    query: '''
      select first 40
        e.emp_no,
        e.first_name,
        e.last_name,
        d.department,
        j.job_title
      from employee e
      join department d on d.dept_no = e.dept_no
      join job j on j.job_code = e.job_code
        and j.job_grade = e.job_grade
        and j.job_country = e.job_country
      where e.emp_no between @empStart and @empEnd
      order by e.emp_no
      ''',
    namedParameters: <String, Object?>{'empStart': 1, 'empEnd': 200},
    tags: <String>['join', 'directory', 'parameterized'],
  ),
  FirebirdBenchmarkScenario(
    database: 'employee',
    name: 'project_staff_rollup',
    description:
        'Project staffing rollup across employee_project, project, and employee.',
    query: '''
      select first 20
        p.proj_id,
        count(*) as assignment_count,
        min(e.hire_date) as first_hire_date
      from project p
      join employee_project ep on ep.proj_id = p.proj_id
      join employee e on e.emp_no = ep.emp_no
      where e.emp_no between @empStart and @empEnd
      group by p.proj_id
      order by 2 desc, 1
      ''',
    namedParameters: <String, Object?>{'empStart': 1, 'empEnd': 200},
    tags: <String>['aggregate', 'project', 'parameterized'],
  ),
];

List<FirebirdBenchmarkScenario> firebirdDefaultBenchmarkScenariosForDatabase(
  String database,
) {
  return switch (database) {
    'chinook' => firebirdChinookBenchmarkScenarios,
    'employee' => firebirdEmployeeBenchmarkScenarios,
    'northwind' => firebirdNorthwindBenchmarkScenarios,
    _ => throw ArgumentError.value(
      database,
      'database',
      'No default benchmark scenarios are registered for this database.',
    ),
  };
}

List<FirebirdBenchmarkScenario> firebirdResolveBenchmarkScenariosForDatabase(
  String database, {
  Iterable<String>? selectedScenarioNames,
}) {
  final scenarios = firebirdDefaultBenchmarkScenariosForDatabase(database);
  if (selectedScenarioNames == null) return scenarios;

  final requested = selectedScenarioNames
      .map((name) => name.trim())
      .where((name) => name.isNotEmpty)
      .toSet();
  if (requested.isEmpty) return scenarios;

  final available = scenarios.map((scenario) => scenario.name).toSet();
  final unknown = requested.where((name) => !available.contains(name)).toList()
    ..sort();
  if (unknown.isNotEmpty) {
    throw ArgumentError(
      'Unknown benchmark scenarios for $database: ${unknown.join(', ')}.',
    );
  }

  return scenarios
      .where((scenario) => requested.contains(scenario.name))
      .toList(growable: false);
}

int _medianMicroseconds(List<int> sortedMicroseconds) {
  final middle = sortedMicroseconds.length ~/ 2;
  if (sortedMicroseconds.length.isOdd) {
    return sortedMicroseconds[middle];
  }

  return (sortedMicroseconds[middle - 1] + sortedMicroseconds[middle]) ~/ 2;
}

int _percentileMicroseconds(List<int> sortedMicroseconds, double percentile) {
  final clamped = percentile.clamp(0.0, 1.0);
  final rank = ((sortedMicroseconds.length * clamped).ceil() - 1).clamp(
    0,
    sortedMicroseconds.length - 1,
  );
  return sortedMicroseconds[rank];
}
