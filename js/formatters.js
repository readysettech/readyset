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

function domainFormatter(value, row, index) {
  return "<a href=\"domain.html?d=" + value + "\">" + value + "</a>";
}

function readableNanosecondFormatter(value) {
  if (value > 1000 * 1000 * 1000) {
    // > 1sec
    return (value / (1000.0 * 1000.0 * 1000.0)) + " sec";
  } else if (value > 1000 * 1000) {
    // < 1sec > 1msec
    return (value / (1000.0 * 1000.0)) + " msec";
  } else if (value > 1000) {
    // < 1msec > 1µsec
    return (value / 1000.0) + " µsec";
  } else {
    return value + " ns";
  }
}
