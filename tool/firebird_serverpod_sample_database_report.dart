import 'package:firebirdpod/firebirdpod.dart';

Future<void> main(List<String> arguments) async {
  final requested = _parseRequestedDatabases(arguments);
  final targets = firebirdSampleDatabaseTargets
      .where((target) => requested == null || requested.contains(target.name))
      .toList();
  final results = <FirebirdSampleDatabaseValidationResult>[];

  if (targets.isEmpty) {
    throw ArgumentError(
      'No sample databases selected. Use names from firebirdSampleDatabaseTargets.',
    );
  }

  print('# Firebird Sample Database Validation Report');
  print('');
  print('| Database | Kind | Tables | Views | Triggers | Procedures | Sequences | Columns | Unknown types | Unresolved defaults | Generator |');
  print('| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |');

  for (final target in targets) {
    final result = await validateSampleDatabase(target);
    results.add(result);
    print(
      '| ${target.name} | ${target.kind.name} | ${result.tableCount} | '
      '${result.viewCount} | ${result.triggerCount} | ${result.procedureCount} | '
      '${result.sequenceCount} | ${result.columnCount} | '
      '${result.unknownColumns.length} | ${result.unresolvedDefaults.length} | '
      '${result.generatorCompatible ? 'ok' : 'error'} |',
    );
  }

  for (final result in results) {
    print('');
    print('## ${result.target.name}');
    print('');
    print('- Kind: `${result.target.kind.name}`');
    print('- Tables: `${result.tableCount}`');
    print('- Views: `${result.viewCount}`');
    print('- Triggers: `${result.triggerCount}`');
    print('- Procedures: `${result.procedureCount}`');
    print('- Sequences: `${result.sequenceCount}`');
    print('- Columns: `${result.columnCount}`');
    print('- Zero-gap baseline: `${result.passesZeroGapBaseline}`');

    if (result.generatorError != null) {
      print('- Generator error: `${result.generatorError}`');
    }

    if (result.unknownColumns.isNotEmpty) {
      print('- Unknown column types:');
      for (final issue in result.unknownColumns) {
        print('  - `${issue.displayLocation}`: ${issue.detail}');
      }
    }

    if (result.unresolvedDefaults.isNotEmpty) {
      print('- Unresolved defaults:');
      for (final issue in result.unresolvedDefaults) {
        print('  - `${issue.displayLocation}`: `${issue.detail}`');
      }
    }
  }
}

Set<String>? _parseRequestedDatabases(List<String> arguments) {
  final requested = <String>{};

  for (final argument in arguments) {
    if (!argument.startsWith('--database=')) continue;
    final value = argument.substring('--database='.length).trim();
    if (value.isEmpty || value == 'all') return null;
    requested.addAll(
      value.split(',').map((item) => item.trim()).where((item) => item.isNotEmpty),
    );
  }

  return requested.isEmpty ? null : requested;
}
