import 'package:meta/meta.dart';

import '../runtime/firebird_connection.dart';
import '../runtime/firebird_statement.dart';
import '../sql/firebird_parameter_style.dart';

/// One-shot query plan inspection over a Firebird connection.
class FirebirdQueryPlanInspector {
  const FirebirdQueryPlanInspector(this._connection);

  final FirebirdConnection _connection;

  Future<FirebirdQueryPlan> explain(
    String query, {
    bool detailed = true,
  }) async {
    final statement = await _connection.prepare(query);
    try {
      return inspectStatement(statement, detailed: detailed);
    } finally {
      await statement.close();
    }
  }

  Future<FirebirdQueryPlan> inspectStatement(
    FirebirdStatement statement, {
    bool detailed = true,
  }) async {
    final plan = await statement.getPlan(detailed: detailed);
    return FirebirdQueryPlan(
      sourceSql: statement.preparedSql.sourceSql,
      normalizedSql: statement.preparedSql.sql,
      parameterStyle: statement.preparedSql.parameterStyle,
      parameterCount: statement.preparedSql.parameterCount,
      detailed: detailed,
      plan: plan.trim(),
    );
  }
}

extension FirebirdConnectionQueryPlans on FirebirdConnection {
  FirebirdQueryPlanInspector get queryPlans => FirebirdQueryPlanInspector(this);
}

@immutable
class FirebirdQueryPlan {
  const FirebirdQueryPlan({
    required this.sourceSql,
    required this.normalizedSql,
    required this.parameterStyle,
    required this.parameterCount,
    required this.detailed,
    required this.plan,
  });

  final String sourceSql;
  final String normalizedSql;
  final FirebirdParameterStyle parameterStyle;
  final int parameterCount;
  final bool detailed;
  final String plan;

  List<String> get lines => plan
      .split('\n')
      .map((line) => line.trimRight())
      .where((line) => line.isNotEmpty)
      .toList(growable: false);
}
