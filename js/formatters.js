function durationFormatter(value) {
  return value.secs + value.nanos / 1000000000.0;
}

function booleanFormatter(value) {
  if (value) {
    return "<span data-feather=\"check\"></span>";
  } else {
    return "<span data-feather=\"x\"></span>";
  }
}
