function preventDefaults (e) { // eslint-disable-line no-unused-vars
  e.preventDefault()
  e.stopPropagation()
}

function handleFiles (files, listformelem, listdispelem) { // eslint-disable-line no-unused-vars
  listformelem.value = getFileNamesString(files, listformelem.value)
  listdispelem.innerHTML = listformelem.value
}

function getFileNamesString (files, curstring) {
  let fileNamesString = ''
  let cumulativeFileList = []
  if (typeof curstring !== 'undefined' && curstring) {
    cumulativeFileList = curstring.split('\n')
  }
  for (let i = 0; i < files.length; ++i) {
    cumulativeFileList.push(files.item(i).name)
  }
  fileNamesString = cumulativeFileList.sort((a, b) => {
    const itemA = a.toUpperCase() // ignore case
    const itemB = b.toUpperCase()
    if (itemA < itemB) { return -1 }
    if (itemA > itemB) { return 1 }
    return 0
  }).join('\n')
  return fileNamesString
}
