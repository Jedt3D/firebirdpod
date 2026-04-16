enum FirebirdCancelMode {
  disable,
  enable,
  raise,
  abort,
}

extension FirebirdCancelModeWireValue on FirebirdCancelMode {
  int get wireValue => switch (this) {
    FirebirdCancelMode.disable => 1,
    FirebirdCancelMode.enable => 2,
    FirebirdCancelMode.raise => 3,
    FirebirdCancelMode.abort => 4,
  };
}
