/*
set table height dynamically by comparing two values:
  input value for percentage of viewport height
  height of the table (div height)
*/
function setTableHeight (divID, pctVH) { // eslint-disable-line no-unused-vars
  const viewportHeight = window.innerHeight
  // height of the DIV section for the table
  const divHeight = document.getElementById(divID).clientHeight
  let targetHeight
  let tabHeight

  // calculated height based on input value for percentage of viewport height
  if (pctVH > 0 && pctVH < 1) {
    targetHeight = pctVH * viewportHeight
  } else {
  // set as 75% of viewport height
    targetHeight = 0.75 * viewportHeight
  }

  // get table height value required by Boostrap-table for fixed header with pagination
  // fixed header height is 120 px for inspected cases
  if (divHeight < targetHeight) {
    tabHeight = divHeight + 120
  } else {
    tabHeight = targetHeight
  }

  return tabHeight
}
