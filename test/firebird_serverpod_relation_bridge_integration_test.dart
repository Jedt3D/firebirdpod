// ignore_for_file: unused_element_parameter

import 'dart:io';

import 'package:firebirdpod/firebirdpod.dart';
import 'package:serverpod_database/serverpod_database.dart';
import 'package:test/test.dart';

import 'firebird_test_support.dart';

const _companyTableName = 'FIREBIRDPOD_REL_COMPANY';
const _authorTableName = 'FIREBIRDPOD_REL_AUTHOR';
const _bookTableName = 'FIREBIRDPOD_REL_BOOK';

class _CompanyRow implements TableRow<int?> {
  _CompanyRow({required this.id, required this.name});

  factory _CompanyRow.fromJson(Map<String, dynamic> json) {
    return _CompanyRow(id: json['id'] as int?, name: json['name'] as String);
  }

  static final _CompanyTable _table = _CompanyTable();

  @override
  final int? id;

  final String name;

  @override
  _CompanyTable get table => _table;

  @override
  Map<String, dynamic> toJson() => {'id': id, 'name': name};
}

class _AuthorRow implements TableRow<int?> {
  _AuthorRow({
    required this.id,
    required this.name,
    required this.companyId,
    this.company,
    this.books,
  });

  factory _AuthorRow.fromJson(Map<String, dynamic> json) {
    final company = json['company'];
    final books = json['books'];
    return _AuthorRow(
      id: json['id'] as int?,
      name: json['name'] as String,
      companyId: json['companyId'] as int?,
      company: company == null
          ? null
          : _CompanyRow.fromJson(Map<String, dynamic>.from(company as Map)),
      books: books == null
          ? null
          : (books as List)
                .map(
                  (entry) => _BookRow.fromJson(
                    Map<String, dynamic>.from(entry as Map),
                  ),
                )
                .toList(),
    );
  }

  static final _AuthorTable _table = _AuthorTable();

  @override
  final int? id;

  final String name;
  final int? companyId;
  final _CompanyRow? company;
  final List<_BookRow>? books;

  @override
  _AuthorTable get table => _table;

  @override
  Map<String, dynamic> toJson() => {
    'id': id,
    'name': name,
    'companyId': companyId,
    if (company != null) 'company': company!.toJson(),
    if (books != null) 'books': books!.map((entry) => entry.toJson()).toList(),
  };
}

class _BookRow implements TableRow<int?> {
  _BookRow({required this.id, required this.authorId, required this.title});

  factory _BookRow.fromJson(Map<String, dynamic> json) {
    return _BookRow(
      id: json['id'] as int?,
      authorId: json['authorId'] as int,
      title: json['title'] as String,
    );
  }

  static final _BookTable _table = _BookTable();

  @override
  final int? id;

  final int authorId;
  final String title;

  @override
  _BookTable get table => _table;

  @override
  Map<String, dynamic> toJson() => {
    'id': id,
    'authorId': authorId,
    'title': title,
  };
}

class _CompanyTable extends Table<int?> {
  _CompanyTable({super.tableRelation}) : super(tableName: _companyTableName);

  late final ColumnString name = ColumnString('name', this);

  @override
  List<Column> get columns => [id, name];
}

class _AuthorTable extends Table<int?> {
  _AuthorTable({super.tableRelation}) : super(tableName: _authorTableName);

  late final ColumnString name = ColumnString('name', this);
  late final ColumnInt companyId = ColumnInt(
    'company_id',
    this,
    fieldName: 'companyId',
  );

  _CompanyTable? _company;
  _BookTable? _books;
  ManyRelation<_BookTable>? _booksRelation;

  _CompanyTable get company {
    if (_company != null) return _company!;
    _company = createRelationTable(
      relationFieldName: 'company',
      field: companyId,
      foreignField: _CompanyRow._table.id,
      tableRelation: tableRelation,
      createTable: (foreignTableRelation) =>
          _CompanyTable(tableRelation: foreignTableRelation),
    );
    return _company!;
  }

  _BookTable get books {
    if (_books != null) return _books!;
    _books = createRelationTable(
      relationFieldName: 'books',
      field: id,
      foreignField: _BookRow._table.authorId,
      tableRelation: tableRelation,
      createTable: (foreignTableRelation) =>
          _BookTable(tableRelation: foreignTableRelation),
    );
    return _books!;
  }

  ManyRelation<_BookTable> get booksRelation {
    if (_booksRelation != null) return _booksRelation!;
    _booksRelation = ManyRelation<_BookTable>(
      tableWithRelations: books,
      table: _BookTable(tableRelation: books.tableRelation!.lastRelation),
    );
    return _booksRelation!;
  }

  @override
  List<Column> get columns => [id, name, companyId];

  @override
  Table? getRelationTable(String relationField) {
    if (relationField == 'company') return company;
    if (relationField == 'books') return books;
    return null;
  }
}

class _BookTable extends Table<int?> {
  _BookTable({super.tableRelation}) : super(tableName: _bookTableName);

  late final ColumnInt authorId = ColumnInt(
    'author_id',
    this,
    fieldName: 'authorId',
  );
  late final ColumnString title = ColumnString('title', this);

  @override
  List<Column> get columns => [id, authorId, title];
}

class _CompanyInclude extends IncludeObject {
  @override
  Map<String, Include?> get includes => const {};

  @override
  Table get table => _CompanyRow._table;
}

class _BookInclude extends IncludeObject {
  @override
  Map<String, Include?> get includes => const {};

  @override
  Table get table => _BookRow._table;
}

class _BookIncludeList extends IncludeList {
  _BookIncludeList._({
    Expression? where,
    super.limit,
    super.offset,
    super.orderBy,
    super.orderDescending = false,
    super.orderByList,
    _BookInclude? include,
  }) : super(include: include) {
    super.where = where;
  }

  @override
  Map<String, Include?> get includes => include?.includes ?? {};

  @override
  Table get table => _BookRow._table;
}

class _AuthorInclude extends IncludeObject {
  _AuthorInclude._({_CompanyInclude? company, _BookIncludeList? books})
    : _company = company,
      _books = books;

  final _CompanyInclude? _company;
  final _BookIncludeList? _books;

  @override
  Map<String, Include?> get includes => {'company': _company, 'books': _books};

  @override
  Table get table => _AuthorRow._table;
}

class _RelationSerializationManager extends SerializationManagerServer {
  @override
  T deserialize<T>(dynamic data, [Type? t]) {
    t ??= T;
    if (t == _CompanyRow) {
      return _CompanyRow.fromJson(Map<String, dynamic>.from(data)) as T;
    }
    if (t == _AuthorRow) {
      return _AuthorRow.fromJson(Map<String, dynamic>.from(data)) as T;
    }
    if (t == _BookRow) {
      return _BookRow.fromJson(Map<String, dynamic>.from(data)) as T;
    }
    return super.deserialize<T>(data, t);
  }

  @override
  String getModuleName() => 'test';

  @override
  Table? getTableForType(Type t) {
    if (t == _CompanyRow) return _CompanyRow._table;
    if (t == _AuthorRow) return _AuthorRow._table;
    if (t == _BookRow) return _BookRow._table;
    return null;
  }

  @override
  List<TableDefinition> getTargetTableDefinitions() => const [];
}

class _TestDatabaseSession implements DatabaseSession {
  @override
  Transaction? transaction;

  @override
  Database get db => throw UnimplementedError('Database wrapper not needed.');

  @override
  LogQueryFunction? get logQuery => null;

  @override
  LogWarningFunction? get logWarning => null;
}

void main() {
  group('Phase 02 Serverpod relation bridge', () {
    late FirebirdServerpodDatabaseConnection connection;
    final authors = _AuthorRow._table;

    setUpAll(() async {
      registerFirebirdServerpodDialect();
      connection = _buildConnection();

      if (!shouldRunDirectIntegrationTests()) {
        return;
      }

      final session = _TestDatabaseSession();
      await connection.simpleExecute(session, '''
        recreate table $_companyTableName (
          ID integer generated by default as identity primary key,
          NAME varchar(80) not null
        )
        ''');
      await connection.simpleExecute(session, '''
        recreate table $_authorTableName (
          ID integer generated by default as identity primary key,
          NAME varchar(80) not null,
          COMPANY_ID integer,
          constraint FK_REL_AUTHOR_COMPANY
            foreign key (COMPANY_ID) references $_companyTableName(ID)
        )
        ''');
      await connection.simpleExecute(session, '''
        recreate table $_bookTableName (
          ID integer generated by default as identity primary key,
          AUTHOR_ID integer not null,
          TITLE varchar(120) not null,
          constraint FK_REL_BOOK_AUTHOR
            foreign key (AUTHOR_ID) references $_authorTableName(ID)
        )
        ''');
    });

    tearDownAll(() async {
      if (!shouldRunDirectIntegrationTests()) {
        return;
      }

      final session = _TestDatabaseSession();
      for (final sql in [
        'drop table $_bookTableName',
        'drop table $_authorTableName',
        'drop table $_companyTableName',
      ]) {
        try {
          await connection.simpleExecute(session, sql);
        } catch (_) {}
      }
    });

    setUp(() async {
      if (!shouldRunDirectIntegrationTests()) {
        return;
      }

      final session = _TestDatabaseSession();
      await connection.simpleExecute(session, 'delete from $_bookTableName');
      await connection.simpleExecute(session, 'delete from $_authorTableName');
      await connection.simpleExecute(session, 'delete from $_companyTableName');
      await connection.simpleExecute(
        session,
        'alter table $_companyTableName alter ID restart with 1',
      );
      await connection.simpleExecute(
        session,
        'alter table $_authorTableName alter ID restart with 1',
      );
      await connection.simpleExecute(
        session,
        'alter table $_bookTableName alter ID restart with 1',
      );
      await _seedRows(connection);
    });

    test(
      'find materializes object and list includes across relation graphs',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          return;
        }

        final session = _TestDatabaseSession();
        final rows = await connection.find<_AuthorRow>(
          session,
          orderBy: authors.id,
          include: _AuthorInclude._(
            company: _CompanyInclude(),
            books: _BookIncludeList._(
              orderBy: _BookRow._table.title.desc(),
              include: _BookInclude(),
            ),
          ),
        );

        expect(rows, hasLength(3));

        expect(rows[0].name, 'Alice');
        expect(rows[0].company?.name, 'Acme');
        expect(rows[0].books?.map((book) => book.title), [
          'Serverpod on Firebird',
          'Firebird Deep Dive',
        ]);

        expect(rows[1].name, 'Bob');
        expect(rows[1].company, isNull);
        expect(rows[1].books?.map((book) => book.title), ['Bob Basics']);

        expect(rows[2].name, 'Cara');
        expect(rows[2].company?.name, 'Contoso');
        expect(rows[2].books, isEmpty);
      },
    );

    test(
      'findById keeps nullable object includes null when the relation is missing',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          return;
        }

        final session = _TestDatabaseSession();
        final row = await connection.findById<_AuthorRow>(
          session,
          2,
          include: _AuthorInclude._(company: _CompanyInclude()),
        );

        expect(row?.name, 'Bob');
        expect(row?.company, isNull);
      },
    );

    test('find filters by included object relation columns', () async {
      if (!shouldRunDirectIntegrationTests()) {
        return;
      }

      final session = _TestDatabaseSession();
      final rows = await connection.find<_AuthorRow>(
        session,
        where: authors.company.name.equals('Acme'),
        include: _AuthorInclude._(company: _CompanyInclude()),
      );

      expect(rows, hasLength(1));
      expect(rows.single.name, 'Alice');
      expect(rows.single.company?.name, 'Acme');
    });

    test('find orders by included object relation columns', () async {
      if (!shouldRunDirectIntegrationTests()) {
        return;
      }

      final session = _TestDatabaseSession();
      final rows = await connection.find<_AuthorRow>(
        session,
        where: authors.companyId.notEquals(null),
        orderBy: authors.company.name.desc(),
        include: _AuthorInclude._(company: _CompanyInclude()),
      );

      expect(rows.map((row) => row.name), ['Cara', 'Alice']);
      expect(rows.map((row) => row.company?.name), ['Contoso', 'Acme']);
    });

    test('find filters by object relation columns through hidden auto-joins', () async {
      if (!shouldRunDirectIntegrationTests()) {
        return;
      }

      final session = _TestDatabaseSession();
      final rows = await connection.find<_AuthorRow>(
        session,
        where: authors.company.name.equals('Acme'),
      );

      expect(rows, hasLength(1));
      expect(rows.single.name, 'Alice');
      expect(rows.single.company, isNull);
    });

    test('find orders by object relation columns through hidden auto-joins', () async {
      if (!shouldRunDirectIntegrationTests()) {
        return;
      }

      final session = _TestDatabaseSession();
      final rows = await connection.find<_AuthorRow>(
        session,
        where: authors.companyId.notEquals(null),
        orderBy: authors.company.name.desc(),
      );

      expect(rows.map((row) => row.name), ['Cara', 'Alice']);
      expect(rows.every((row) => row.company == null), isTrue);
    });

    test('find filters by list relation count and any semantics', () async {
      if (!shouldRunDirectIntegrationTests()) {
        return;
      }

      final session = _TestDatabaseSession();

      final countedRows = await connection.find<_AuthorRow>(
        session,
        where: authors.booksRelation.count() > 1,
        orderBy: authors.id,
      );
      final anyRows = await connection.find<_AuthorRow>(
        session,
        where: authors.booksRelation.any(
          (books) => books.title.like('%Firebird%'),
        ),
        orderBy: authors.id,
      );

      expect(countedRows.map((row) => row.name), ['Alice']);
      expect(anyRows.map((row) => row.name), ['Alice']);
    });

    test('count supports list relation filters', () async {
      if (!shouldRunDirectIntegrationTests()) {
        return;
      }

      final session = _TestDatabaseSession();
      final rowCount = await connection.count<_AuthorRow>(
        session,
        where: authors.booksRelation.any(
          (books) => books.title.like('%Firebird%'),
        ),
      );

      expect(rowCount, 1);
    });

    test('find orders by list relation count', () async {
      if (!shouldRunDirectIntegrationTests()) {
        return;
      }

      final session = _TestDatabaseSession();
      final rows = await connection.find<_AuthorRow>(
        session,
        orderBy: authors.booksRelation.count().desc(),
      );

      expect(rows.map((row) => row.name), ['Alice', 'Bob', 'Cara']);
    });

    test('find paginates IncludeList per parent with limit', () async {
      if (!shouldRunDirectIntegrationTests()) {
        return;
      }

      final session = _TestDatabaseSession();

      final rows = await connection.find<_AuthorRow>(
        session,
        orderBy: authors.id,
        include: _AuthorInclude._(
          books: _BookIncludeList._(
            limit: 1,
            orderBy: _BookRow._table.title.desc(),
          ),
        ),
      );

      expect(rows, hasLength(3));
      expect(rows[0].books?.map((book) => book.title), [
        'Serverpod on Firebird',
      ]);
      expect(rows[1].books?.map((book) => book.title), ['Bob Basics']);
      expect(rows[2].books, isEmpty);
    });

    test(
      'find paginates IncludeList per parent with offset and limit',
      () async {
        if (!shouldRunDirectIntegrationTests()) {
          return;
        }

        final session = _TestDatabaseSession();

        final rows = await connection.find<_AuthorRow>(
          session,
          orderBy: authors.id,
          include: _AuthorInclude._(
            books: _BookIncludeList._(
              limit: 1,
              offset: 1,
              orderBy: _BookRow._table.title.desc(),
            ),
          ),
        );

        expect(rows, hasLength(3));
        expect(rows[0].books?.map((book) => book.title), [
          'Firebird Deep Dive',
        ]);
        expect(rows[1].books, isEmpty);
        expect(rows[2].books, isEmpty);
      },
    );
  });
}

FirebirdServerpodDatabaseConnection _buildConnection() {
  final provider =
      DatabaseProvider.forDialect(DatabaseDialect.firebird)
          as FirebirdServerpodDatabaseProvider;
  final poolManager = provider.createPoolManager(
    _RelationSerializationManager(),
    null,
    FirebirdServerpodDatabaseConfig(
      host: Platform.environment['FIREBIRDPOD_TEST_HOST'] ?? 'localhost',
      port: _testPort(),
      user: firebirdTestUser(),
      password: firebirdTestPassword(),
      name: firebirdTestDatabasePath(),
      fbClientLibraryPath: firebirdClientLibraryPath(),
    ),
  );
  return provider.createConnection(poolManager);
}

Future<void> _seedRows(FirebirdServerpodDatabaseConnection connection) async {
  final session = _TestDatabaseSession();
  final inserts = <String>[
    "insert into $_companyTableName (NAME) values ('Acme')",
    "insert into $_companyTableName (NAME) values ('Contoso')",
    "insert into $_authorTableName (NAME, COMPANY_ID) values ('Alice', 1)",
    "insert into $_authorTableName (NAME, COMPANY_ID) values ('Bob', NULL)",
    "insert into $_authorTableName (NAME, COMPANY_ID) values ('Cara', 2)",
    "insert into $_bookTableName (AUTHOR_ID, TITLE) values (1, 'Firebird Deep Dive')",
    "insert into $_bookTableName (AUTHOR_ID, TITLE) values (1, 'Serverpod on Firebird')",
    "insert into $_bookTableName (AUTHOR_ID, TITLE) values (2, 'Bob Basics')",
  ];

  for (final sql in inserts) {
    await connection.simpleExecute(session, sql);
  }
}

int _testPort() {
  final value = Platform.environment['FIREBIRDPOD_TEST_PORT'];
  if (value == null || value.isEmpty) return 3050;
  return int.parse(value);
}
