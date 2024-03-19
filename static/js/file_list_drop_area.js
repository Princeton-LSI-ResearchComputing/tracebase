var dropArea = null // eslint-disable-line no-var
var listformelem = null // eslint-disable-line no-var
var listdispelem = null // eslint-disable-line no-var

/**
 * This initializes all of the global variables.
 */
function initDropArea (dropArea, listformelem, listdispelem) { // eslint-disable-line no-unused-vars
  ;['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
    dropArea.addEventListener(eventName, preventDefaults, false)
  })

  ;['dragenter', 'dragover'].forEach(eventName => {
    dropArea.addEventListener(eventName, highlight, false)
  })

  ;['dragleave', 'drop'].forEach(eventName => {
    dropArea.addEventListener(eventName, unhighlight, false)
  })

  dropArea.addEventListener('drop', handleDrop, false)

  globalThis.dropArea = dropArea
  globalThis.listformelem = listformelem
  globalThis.listdispelem = listdispelem

  refreshMzXMLDisplayList()
}

function preventDefaults (e) { // eslint-disable-line no-unused-vars
  e.preventDefault()
  e.stopPropagation()
}

function highlight (e) {
  dropArea.classList.add('highlight')
}

function unhighlight (e) {
  dropArea.classList.remove('highlight')
}

function handleDrop (e) {
  const dt = e.dataTransfer
  const files = dt.files

  handleFiles(files)
}

function handleFiles (files) { // eslint-disable-line no-unused-vars
  listformelem.value = getFileNamesString(files, listformelem.value)
  refreshMzXMLDisplayList()
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

function refreshMzXMLDisplayList () {
  listdispelem.innerHTML = listformelem.value
}
