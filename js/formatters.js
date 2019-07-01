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

function instanceFormatter(value, row, index) {
  return "<a href=\"nodes.html?i=" + value + "\">" + value + "</a>";
}

function nodeFormatter(value, row, index) {
  return "<a href=\"node.html?n=" + value + "\">" + value + "</a>";
}
