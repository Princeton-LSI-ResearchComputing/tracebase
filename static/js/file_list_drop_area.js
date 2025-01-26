var dropArea = null // eslint-disable-line no-var
var fileFunc = null // eslint-disable-line no-var
var postDropFunc = null // eslint-disable-line no-var
const allFiles = [] // eslint-disable-line no-unused-vars
const newFiles = [] // eslint-disable-line no-unused-vars

// This code is based on the following article:
// https://www.smashingmagazine.com/2018/01/drag-drop-file-uploader-vanilla-js/

/**
 * This initializes all of the global variables.
 * @param {*} dropArea is the div element where files are dropped
 * @param {*} fileFunc is a function that takes DataTransfer object containing a single file.  It will be called for
 *   every dropped file.
 * @param {*} postDropFunc is an optional function without arguments that is called after all the files have been
 *   processed.
 */
function initDropArea (dropArea, fileFunc, postDropFunc) { // eslint-disable-line no-unused-vars
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
  if (typeof fileFunc !== 'undefined' && fileFunc) {
    globalThis.fileFunc = fileFunc
  } else {
    globalThis.fileFunc = null
  }
  if (typeof postDropFunc !== 'undefined' && postDropFunc) {
    globalThis.postDropFunc = postDropFunc
  } else {
    globalThis.postDropFunc = null
  }

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
 * This method initializes and maintains a global list of all and new files and creates a DataTransfer object for every
 * individual dropped/picked file and calls the fileFunc that was initialized by the initDropArea function.
 * @param {*} files - An optional array of file objects.  If null, the global lists are emptied.
 */
function handleFiles (files) { // eslint-disable-line no-unused-vars
  if (typeof files === 'undefined' || !files) {
    globalThis.allFiles = []
    globalThis.newFiles = []
  } else {
    globalThis.newFiles = files
    for (let i = 0; i < files.length; ++i) {
      globalThis.allFiles.push(files.item(i))
      // See: https://stackoverflow.com/questions/8006715/
      const dT = new DataTransfer() // eslint-disable-line no-undef
      dT.items.add(files.item(i))
      if (typeof fileFunc !== 'undefined' && fileFunc) {
        fileFunc(dT)
      }
    }
    if (typeof postDropFunc !== 'undefined' && postDropFunc) {
      postDropFunc(files)
    }
  }
}
