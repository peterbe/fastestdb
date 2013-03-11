$(function() {
  // color code the slowest and fastest in each column
  var creates = [], edits = [], dels = [], totals = [];

  $('tbody tr').each(function() {
    var td = $('td', this);
    creates.push(parseFloat(td.eq(1).text()));
    edits.push(parseFloat(td.eq(2).text()));
    dels.push(parseFloat(td.eq(3).text()));
    totals.push(parseFloat(td.eq(4).text()));
  });
  var _sort = function(a, b) { return a - b; };
  creates.sort(_sort);
  edits.sort(_sort);
  dels.sort(_sort);
  totals.sort(_sort);
  var f_create = creates[0], f_edit = edits[0], f_del = dels[0], f_total = totals[0];
  var last = creates.length - 1;
  var s_create = creates[last], s_edit = edits[last], s_del = dels[last], s_total = totals[last];

  $('tbody tr').each(function() {
    var td = $('td', this);
    var create = parseFloat(td.eq(1).text());
    var edit = parseFloat(td.eq(2).text());
    var del = parseFloat(td.eq(3).text());
    var total = parseFloat(td.eq(4).text());
    if (create === f_create) {
      td.eq(1).addClass('fastest');
    } else if (create == s_create) {
      td.eq(1).addClass('slowest');
    }
    if (edit === f_edit) {
      td.eq(2).addClass('fastest');
    } else if (edit == s_edit) {
      td.eq(2).addClass('slowest');
    }
    if (del === f_del) {
      td.eq(3).addClass('fastest');
    } else if (del == s_del) {
      td.eq(3).addClass('slowest');
    }
    if (total === f_total) {
      td.eq(4).addClass('fastest');
    } else if (total == s_total) {
      td.eq(4).addClass('slowest');
    }
  });

  $('table').tablesorter({
    widgets        : ['zebra', 'columns'],
    sortList: [[4,0]]
  });

  var data = [];
  $('tbody tr').each(function() {
    data.push([$('td', this).eq(0).text(), parseFloat($('td', this).eq(-1).text())]);
  });

  $.plot("#placeholder", [ data ], {
    series: {
      bars: {
        show: true,
        barWidth: 0.7,
        align: "center"
     }
    },
    yaxis: {
      axisLabel: 'seconds'
    },
    xaxis: {
      mode: "categories",
      tickLength: 0
    }
  });

});
