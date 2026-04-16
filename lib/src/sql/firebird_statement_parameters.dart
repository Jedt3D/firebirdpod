sealed class FirebirdStatementParameters {
  const FirebirdStatementParameters();

  factory FirebirdStatementParameters.named(Map<String, Object?> parameters) =
      FirebirdNamedParameters;

  factory FirebirdStatementParameters.positional(List<Object?> parameters) =
      FirebirdPositionalParameters;
}

final class FirebirdNamedParameters extends FirebirdStatementParameters {
  const FirebirdNamedParameters(this.parameters);

  final Map<String, Object?> parameters;
}

final class FirebirdPositionalParameters extends FirebirdStatementParameters {
  const FirebirdPositionalParameters(this.parameters);

  final List<Object?> parameters;
}
