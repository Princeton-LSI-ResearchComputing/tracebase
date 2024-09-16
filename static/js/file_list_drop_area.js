var dropArea = null // eslint-disable-line no-var
var fileElem = null // eslint-disable-line no-var
var allFiles = []
var newFiles = []

/**
 * This initializes all of the global variables.
 */
function initDropArea (dropArea, fileElem) { // eslint-disable-line no-unused-vars
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
  globalThis.fileElem = fileElem

  handleFiles(null)
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

/**
 * This method initializes and maintains a global list of all and new files.
 * @param {*} files - An optional array of file objects.  If null, the global lists are emptied.
 */
function handleFiles (files) { // eslint-disable-line no-unused-vars
  if (typeof files === 'undefined' || !files) {
    globalThis.allFiles = []
    globalThis.newFiles = []
  } else {
    globalThis.newFiles = files
    for (let i = 0; i < files.length; ++i) {
      // See: https://stackoverflow.com/questions/8006715/
      // TODO: Change this to get the right file input element.  This is currently just a proof of concept.
      addFilesToUpload([files.item(i)])
      globalThis.allFiles.push(files.item(i))
      console.log("Setting the", globalThis.fileElem.name, "input element to", files.item(i).name)
    }
  }
}

function addFilesToUpload(files) { // eslint-disable-line no-unused-vars
  // files is a regular array (or list?) of file objects
  // FileList objects (like globalThis.fileElem.files) are read-only
  const dT = new DataTransfer();
  // TODO: For now, do not add previous files.  Once the form cloning is done, this can be used (but will need to be edited to use 1 file per input element)
  // // Add previously added files to the DataTranfer object
  // for (let i = 0; i < globalThis.fileElem.files.length; i++) {
  //     dT.items.add(globalThis.fileElem.files[i]);
  // }
  // Add newly selected files to the DataTranfer object
  for (let i = 0; i < files.length; i++) {
      dT.items.add(files[i]);
  }
  // Replace the FileList object in the file input element with the combined (previously added and new) files
  globalThis.fileElem.files = dT.files;
  // Save the FileList object for use later when the user drops more files in
  current_peak_annot_files = globalThis.fileElem.files;
}

function getFileNamesString (files, curstring) {
  let fileNamesString = ''
  let cumulativeFileList = []
  // If the current string is populated and we're not clearing the file list
  if (typeof curstring !== 'undefined' && curstring && files.length > 0) {
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
