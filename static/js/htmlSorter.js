// sort column based on HTML values
// ref: https://github.com/wenzhixin/bootstrap-table/issues/461
// disable eslint in order to make sorting work with Bootstrap-Table, can't replace "var" with "let"
/* eslint-disable */
function htmlSorter(a, b) {
  var a = $(a).text();
  var b = $(b).text();
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}
/* eslint-enable */
