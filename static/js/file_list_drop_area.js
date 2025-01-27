// A dictionary of dropArea info, e.g. dropAreas["dropAreakKey"] = {
//     "dropArea": dropArea,
//     "fileFunc": fileFunc,
//     "postDropFunc": postDropFunc
// }
var dropAreas = {} // eslint-disable-line no-unused-vars

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
function initDropArea (dropAreaKey, dropArea, fileFunc, postDropFunc) { // eslint-disable-line no-unused-vars
  ;['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
    dropArea.addEventListener(eventName, preventDefaults, false)
  })

  ;['dragenter', 'dragover'].forEach(eventName => {
    dropArea.addEventListener(eventName, highlight, false,)
  })

  ;['dragleave', 'drop'].forEach(eventName => {
    dropArea.addEventListener(eventName, unhighlight, false)
  })

  dropArea.addEventListener('drop', function (e) {handleDrop(e, dropAreaKey)}, false)

  globalThis.dropAreas[dropAreaKey] = {
    "dropArea": dropArea,
    "fileFunc": null,
    "postDropFunc": null
  }

  if (typeof fileFunc !== 'undefined' && fileFunc) {
    globalThis.dropAreas[dropAreaKey]["fileFunc"] = fileFunc
  }
  
  if (typeof postDropFunc !== 'undefined' && postDropFunc) {
    globalThis.dropAreas[dropAreaKey]["postDropFunc"] = postDropFunc
  }
}

function preventDefaults (e) { // eslint-disable-line no-unused-vars
  e.preventDefault()
  e.stopPropagation()
}

function highlight (e) {
  this.classList.add('highlight')
}

function unhighlight (e) {
  this.classList.remove('highlight')
}

function handleDrop (event, dropAreaKey) {
  const dt = event.dataTransfer
  const files = dt.files
  handleFiles(files, dropAreaKey)
}

/**
 * This method calls the fileFunc that was initialized by the initDropArea function on each file and then calls the
 * postDropFunc, passing all files.
 * @param {*} files - An optional array of file objects.
 */
function handleFiles (files, dropAreaKey) { // eslint-disable-line no-unused-vars
  fileFunc = dropAreas[dropAreaKey]["fileFunc"];
  postDropFunc = dropAreas[dropAreaKey]["postDropFunc"];
  if (typeof files !== 'undefined' && files) {
    for (let i = 0; i < files.length; ++i) {
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
