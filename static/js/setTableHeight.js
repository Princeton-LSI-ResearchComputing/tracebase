/*
set table height dynamically by comparing two values:
  input value for percentage of viewport height
  height of the table (div height)
*/
function setTableHeight (divID, pctVH) {
  let viewportHeight = window.innerHeight
  let targetHeight
  let divHeight
  let tabHeight

  // calculated height based on input value for percentage of viewport height
  if (pctVH > 0 && pctVH < 1) {
    targetHeight = pctVH * viewportHeight
  } else {
  // set as 75% of viewport height
    targetHeight = 0.75 * viewportHeight
  }

  // height of the DIV section for the table
  divHeight = document.getElementById(divID).clientHeight

  // get table height value required by Boostrap-table for fixed header feature
  if (divHeight < targetHeight) {
    tabHeight = divHeight
  } else {
    tabHeight = targetHeight
  }

  return tabHeight
}
