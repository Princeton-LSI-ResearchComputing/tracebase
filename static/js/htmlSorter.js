// sort column based on HTML values
// ref: https://github.com/wenzhixin/bootstrap-table/issues/461
function htmlSorter(a, b) {// eslint-disable-line no-unused-vars
  /* eslint-env jquery */
  a = $(a).text();
  b = $(b).text();
  if (a < b) return -1;
  if (a > b) return 1;
  return 0;
}
