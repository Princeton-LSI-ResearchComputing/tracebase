var dropArea = null // eslint-disable-line no-var
var fileFunc = null // eslint-disable-line no-var
var postDropFunc = null // eslint-disable-line no-var
var allFiles = []
var newFiles = []

// This code is based on the following article:
// https://www.smashingmagazine.com/2018/01/drag-drop-file-uploader-vanilla-js/

/**
 * This initializes all of the global variables.
 * - dropArea is the div element where files are dropped
 * - fileFunc is a function that takes DataTransfer object containing a single file.  It will be called for every
 *   dropped file.
 * - postDropFunc is an optional function without arguments that is called after all the files have been processed.
 */
function initDropArea (dropArea, fileFunc, postDropFunc) { // eslint-disable-line no-unused-vars
  ;['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
    dropArea.addEventListener(eventName, preventDefaults, false)
  });

  ;['dragenter', 'dragover'].forEach(eventName => {
    dropArea.addEventListener(eventName, highlight, false)
  });

  ;['dragleave', 'drop'].forEach(eventName => {
    dropArea.addEventListener(eventName, unhighlight, false)
  });

  dropArea.addEventListener('drop', handleDrop, false);

  console.log("Setting dropArea to", dropArea);
  console.log("Setting fileFunc to", fileFunc);
  globalThis.dropArea = dropArea;
  globalThis.fileFunc = fileFunc;
  if (typeof postDropFunc !== 'undefined' && postDropFunc) {
    console.log("Setting postDropFunc to", postDropFunc);
    globalThis.postDropFunc = postDropFunc
  }

  handleFiles(null);
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
      const dT = new DataTransfer();
      dT.items.add(files.item(i));
      fileFunc(dT)
    }
    if (typeof postDropFunc !== 'undefined' && postDropFunc) {
      postDropFunc()
    }
  }
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
