var peakAnnotFormTemplateContainer = null // eslint-disable-line no-var
var peakAnnotFormsTable = null // eslint-disable-line no-var
var peakAnnotDropAreaInput = null // eslint-disable-line no-var
var dataSubmissionForm = null // eslint-disable-line no-var
var singleFormDiv = null // eslint-disable-line no-var
var studyDocInput = null // eslint-disable-line no-var
var annotFilesInput = null // eslint-disable-line no-var

var mzxmlFormTemplateContainer = null // eslint-disable-line no-var
var mzxmlFormsTable = null // eslint-disable-line no-var
var mzxmlDirDropAreaInput = null // eslint-disable-line no-var
var mzxmlSubmissionForm = null // eslint-disable-line no-var
var mzxmlFileListDisplayElem = null // eslint-disable-line no-var

/**
 * A method to initialize the peak annotation file form interface.  Dropped files will call addPeakAnnotFileToUpload,
 * and that method needs 2 things that this method initializes:
 * @param {*} templateContainer [tr] A table row containing form input elements for the peak annotation file and the
 * sequence metadata.
 * @param {*} peakAnnotFormsTable [table] The table where the form rows will be added when files are dropped in the drop
 * area.
 */
function initPeakAnnotUploads (peakAnnotFormTemplateContainer, peakAnnotFormsTable, peakAnnotDropAreaInput, dataSubmissionForm, singleFormDiv, studyDocInput, annotFilesInput) { // eslint-disable-line no-unused-vars
  globalThis.peakAnnotFormTemplateContainer = peakAnnotFormTemplateContainer
  globalThis.peakAnnotFormsTable = peakAnnotFormsTable
  globalThis.peakAnnotDropAreaInput = peakAnnotDropAreaInput
  globalThis.dataSubmissionForm = dataSubmissionForm
  globalThis.singleFormDiv = singleFormDiv
  globalThis.studyDocInput = studyDocInput
  globalThis.annotFilesInput = annotFilesInput

  // Add an event listener to the study doc input to enable the submit button
  studyDocInput.addEventListener('change', function () {
    enablePeakAnnotForm()
  })

  // Disable the form submission button to start (because there are no peak annotation form rows yet).
  disablePeakAnnotForm()
}

function initMzxmlMetadataUploads (mzxmlFormTemplateContainer, mzxmlFormsTable, mzxmlDirDropAreaInput, mzxmlSubmissionForm, singleFormDiv, studyDocInput) { // eslint-disable-line no-unused-vars
  globalThis.mzxmlFormTemplateContainer = mzxmlFormTemplateContainer
  globalThis.mzxmlFormsTable = mzxmlFormsTable
  globalThis.mzxmlDirDropAreaInput = mzxmlDirDropAreaInput
  globalThis.mzxmlSubmissionForm = mzxmlSubmissionForm
  // TODO: Add support for supplying a study doc to "update". These study doc elements are placeholders that came from
  // TODO: copying the start page but in this context (mzxml autofill), they are vestigial.
  globalThis.singleFormDiv = singleFormDiv
  globalThis.studyDocInput = studyDocInput
  // TODO: Add ability to disable/enable the submit button
}

/**
 * This method takes a single peak annotation file for upload (inside a DataTransfer object) and clones a file upload
 * form for a single file input along with sequence metadata inputs and un-hides the file input.
 * @param {*} file A file object
 */
function addPeakAnnotFileToUpload (file) { // eslint-disable-line no-unused-vars
  // Create a row for the metadata inputs associated with each file
  const newRow = createPeakAnnotFormRow()
  makeAnnotFormModifications(file, newRow)
  peakAnnotFormsTable.appendChild(newRow)
}
// TODO: There needs to exist a way to add mzxml form rows, but it cannot be per file.  The directory picker does not result in a selected directory for upload, it selects all files under the directory for upload, so what is needed is a way to compute each first directory that contains mzXML files and a form row for each directory must be created.  So we need a "addMzxmlDirsToUpload" that determines the directories, calls createPeakAnnotFormRow for each one, and calls refreshMzxmlMetadata for each one

/**
 * This function clones a tr row containing the form elements for a peak annotation file.
 * @param {*} template - The tr element for the template to clone.
 * @returns the cloned row element containing the cloned form for a peak annot file.
 */
function createPeakAnnotFormRow (template) {
  if (typeof template === 'undefined' || !template) {
    template = peakAnnotFormTemplateContainer
  }
  return template.cloneNode(true)
}
// TODO: Create a createMzxmlFormRow function

/**
 * Un-hide the file input column and set the files of the file input.
 * @param {*} file - A file object.
 * @param {*} formRow - The row element containing the form.
 */
function makeAnnotFormModifications (file, formRow) {
  // Un-hide the columns with the UnHideMe class
  const fileTds = formRow.querySelectorAll('td')
  for (let i = 0; i < fileTds.length; i++) {
    const fileTd = fileTds[i]
    if (fileTd.classList.contains('UnHideMe')) {
      fileTd.style = null
      fileTd.classList.remove('UnHideMe');
    }
  }

  // Set the file for the file input
  const annotFileNameSpan = formRow.querySelector('span[name="peak_annotation_file"]')
  annotFileNameSpan.innerHTML = file.name

  // Remove the ID (which is what is used to identify the row template)
  formRow.removeAttribute('id')

  // TODO: Attach listeners to the select list elements to make updates to the peak_annot_to_mzxml_metadata JSON field
  // when the selection changes

  // TODO: Eventually, we could automatically edit the population of the sequence_dir and scan_dir select lists and make
  // a selection based on colocation of a peak annotation file matching the name of the file here with a sequence/scan
  // dir it's in.  That would depend on identifying all possible peak annotation files from the direcvtory walk in the
  // raw file input.  For now though, the lists are manual.
}

/**
 * Call this upon subit to turn a series of cloned regular forms in tr tags into a FormSet.
 */
function onSubmit () { // eslint-disable-line no-unused-vars
  const numForms = prepareFormsetForms()
  insertFormsetManagementInputs(numForms)
}

/**
 * This function edits any input element containing class form-control into a django FormSet conpatible input field.
 * @returns the number of forms in the formset
 */
function prepareFormsetForms () {
  // Check if a study doc is being submitted
  const studyDocExists = studyDocInput.files.length > 0

  // Process the form elements that only occur once (i.e. are not replicated)
  const singlePrefix = 'form-0-'
  const singleInputElems = singleFormDiv.querySelectorAll('input')
  for (let i = 0; i < singleInputElems.length; i++) {
    const singleInputElem = singleInputElems[i]
    // If this input element contains the form-control class (i.e. we're using the presence of the form-control class
    // to infer that the element is an explicitly added input element (not some shadow element)
    if (singleInputElem.classList.contains('form-control')) {
      if (singleInputElem.for) {
        singleInputElem.for = singlePrefix + singleInputElem.for
      }
      if (singleInputElem.id) {
        singleInputElem.id = singlePrefix + singleInputElem.id
      }
      if (singleInputElem.name) {
        singleInputElem.name = singlePrefix + singleInputElem.name
      }
    }
  }

  // Process the form elements that occur multiple times (i.e. are replicated)
  const formRows = getFormRows()
  for (let r = 0; r < formRows.length; r++) {
    const formRow = formRows[r]
    const inputElems = formRow.querySelectorAll('input')
    // Prepend attributes 'id', 'name', and 'for' with "form-0-", as is what django expects from a formset
    const multiPrefix = 'form-' + r.toString() + '-'
    for (let i = 0; i < inputElems.length; i++) {
      const inputElem = inputElems[i]
      // If this input element contains the form-control class (i.e. we're using the presence of the form-control class
      // to infer that the element is an explicitly added input element (not some shadow element)
      if (inputElem.classList.contains('form-control')) {
        if (inputElem.for) {
          inputElem.for = multiPrefix + inputElem.for
        }
        if (inputElem.id) {
          inputElem.id = multiPrefix + inputElem.id
        }
        if (inputElem.name) {
          inputElem.name = multiPrefix + inputElem.name
        }
      }
    }
  }

  if (formRows.length > 0) {
    return formRows.length
  } else if (studyDocExists) {
    return 1
  }
  return 0
}

/**
 * This function adds form management inputs to the FormSet (for total and initial forms).  Note, min and max are not
 * required for processing a django FormSet.
 * @param {*} numForms [integer] The number of forms in the FormSet
 */
function insertFormsetManagementInputs (numForms) {
  // dataSubmissionForm.innerHTML += '<input type="hidden" name="form-TOTAL_FORMS" ';
  // dataSubmissionForm.innerHTML += 'value="' + numForms.toString() + '" id="id_form-TOTAL_FORMS">';
  const totalInput = document.createElement('input')
  totalInput.setAttribute('type', 'hidden')
  totalInput.setAttribute('name', 'form-TOTAL_FORMS')
  totalInput.setAttribute('value', numForms.toString())
  totalInput.setAttribute('id', 'id_form-TOTAL_FORMS')
  dataSubmissionForm.appendChild(totalInput)

  // dataSubmissionForm.innerHTML += '<input type="hidden" name="form-INITIAL_FORMS" value="0" ';
  // dataSubmissionForm.innerHTML += 'id="id_form-INITIAL_FORMS">';
  const initialInput = document.createElement('input')
  initialInput.setAttribute('type', 'hidden')
  initialInput.setAttribute('name', 'form-INITIAL_FORMS')
  initialInput.setAttribute('value', '0')
  initialInput.setAttribute('id', 'id_form-INITIAL_FORMS')
  dataSubmissionForm.appendChild(initialInput)
}

/**
 * This method returns all tr elements contaning a form belonging to the formset we are creating.
 * @returns all tr elements containing an individual (cloned) form.
 */
function getFormRows () {
  return peakAnnotFormsTable.querySelectorAll('tr[name="form-set-row"]')
}

/**
 * This function enables the form submission button.
 */
function enablePeakAnnotForm () {
  const submitInput = document.querySelector('#submit')
  submitInput.removeAttribute('disabled')
}

/**
 * This function disables the form submission button.
 */
function disablePeakAnnotForm () {
  const submitInput = document.querySelector('#submit')
  submitInput.disabled = true
}

/**
 * This function clears the file picker input element inside the drop area after having created form rows.  It is called
 * from the annot-drop-area code after all dropped/picked files have been processed.  It intentionally leaves the
 * entries in the sequence metadata inputs for re-use upon additional drops/picks.
 * @param {*} newFiles [list of files]: The list of files from a DataTransfer object containing the newly dropped/selected files
 */
function afterAddingPeakAnnotFiles (newFiles) { // eslint-disable-line no-unused-vars
  
  // Add the files to the hidden annotFilesInput
  // See: https://stackoverflow.com/questions/8006715/
  // Create a new DataTransfer object to contain the merged list of files
  const newDT = new DataTransfer();
  let filenames = []
  let dupes = []
  // Add the existing files
  for (i=0;i < globalThis.annotFilesInput.files.length;i++) {
    if (filenames.includes(globalThis.annotFilesInput.files[i].name)) {
      dupes.push(globalThis.annotFilesInput.files[i].name)
    } else {
      newDT.items.add(globalThis.annotFilesInput.files[i]);
      filenames.push(globalThis.annotFilesInput.files[i].name)
    }
  }
  // Add the incoming files
  for (i=0;i < newFiles.length;i++) {
    if (filenames.includes(newFiles[i].name)) {
      dupes.push(newFiles[i].name)
    } else {
      newDT.items.add(newFiles[i]);
      filenames.push(newFiles[i].name)
      addPeakAnnotFileToUpload(newFiles[i])
    }
  }
  // Now replace the input element's old files with the merged new files
  globalThis.annotFilesInput.files = newDT.files;
  
  // Clear the drop area to accept new files
  peakAnnotDropAreaInput.value = null

  if (dupes.length > 0) {
    alert("Peak annotation filenames must be unique.  Skipped these files with duplicate names:" + dupes)
  }

  // Enable form submission
  enablePeakAnnotForm()
}

function refreshMzxmlMetadata (files) { // eslint-disable-line no-unused-vars
  filepaths = getAllMzxmlFilePaths(files)
  console.log("filepaths:", filepaths)

  // Update the mzxml_file_list form element
  mzxmlListInput = document.getElementById('mzxml_file_list_input_id')
  mzxmlListInput.value = JSON.stringify(filepaths)

  mzxmldirpaths = getAllMzxmlDirectories(filepaths)
  sequencedirpaths = getParentMzxmlDirectories(mzxmldirpaths)

  // TODO: Build a seqdir select list and a series of scandir select lists keyed on seqdir in the loop below

  // For each sequence directory
  for (i=0;i < sequencedirpaths.length;i++) {
    seqdirpath = sequencedirpaths[i];
    // Create a form row (duplicating the template) and populate the parent directory in default_sequence_dir form elem
    addSequenceFormRow(seqdirpath);
    // getMzxmlsInDirectory
    // Construct string (using .join()) of filepaths - use to populate the mzxml_metadata form elem
  }

  // TODO: Update the peak annotation file row select lists

  // TODO: Populate a list of files for each sequence form row that only display when the user does something (like click a caret or hover or something)
}

function addSequenceFormRow (seqdirpath) {
  let newRow = mzxmlFormTemplateContainer.cloneNode(true)
  makeSequenceFormModifications(newRow, seqdirpath)
  mzxmlFormsTable.appendChild(newRow)
}

function makeSequenceFormModifications (formRow, seqdirpath) {
  // Un-hide the columns with the UnHideMe class
  const fileTds = formRow.querySelectorAll('td')
  for (let i = 0; i < fileTds.length; i++) {
    const fileTd = fileTds[i]
    if (fileTd.classList.contains('UnHideMe')) {
      fileTd.style = null
      fileTd.classList.remove('UnHideMe');
    }
  }

  // Set the file for the file input and don't let the user change it
  const seqDirInput = formRow.querySelector('#sequence_dir_input_id')
  seqDirInput.value = seqdirpath
  // seqDirInput.readonly = true
  seqDirInput.setAttribute('readonly', true)

  console.log("Updated seqdir input:", seqDirInput, "seqdir:", seqdirpath)

  // Remove the ID (which is what is used to identify the row template)
  formRow.removeAttribute('id')
}

/**
 * This function clears all of the previously added peak annotation form rows.
 */
function clearPeakAnnotFiles () { // eslint-disable-line no-unused-vars
  peakAnnotFormsTable.innerHTML = ''
}

function getMzxmlFileNamesString (newFiles, curstring) {
  let fileNamesString = ''

  let cumulativeFileList = []
  if (typeof curstring !== 'undefined' && curstring) {
    cumulativeFileList = curstring.split('\n')
  }

  for (let i = 0; i < newFiles.length; ++i) {

    file_obj = newFiles.item(i)

    if (Object.hasOwn(file_obj, 'webkitRelativePath')) {
      filepath = file_obj.webkitRelativePath
    } else {
      filepath = file_obj.name
    }

    if (!cumulativeFileList.includes(filepath)) {
      cumulativeFileList.push(filepath)
    }
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

function getAllMzxmlFilePaths(files) {
  // See: https://stackoverflow.com/a/60538623/2057516
  let allMzxmlFilePathList = []
  for (let i = 0; i < files.length; ++i) {
    console.log("Checking out file", files[i].name)
    if (!files[i].name.toLowerCase().endsWith(".mzxml")) {
      // We only want directories that directly contain mzXML files
      continue
    }
    if (Object.hasOwn(files[i], 'webkitRelativePath')) {
      filePath = files[i].webkitRelativePath;
    } else {
      filePath = files[i].name
    }
    allMzxmlFilePathList.push(filePath)
  }
  return allMzxmlFilePathList
}

function getAllMzxmlDirectories(filepaths) {
  let mzxmlDirList = []
  for (let i = 0; i < filepaths.length; ++i) {
    if (!filepaths[i].toLowerCase().endsWith(".mzxml")) {
      // We only want directories that directly contain mzXML files
      continue
    }
    filePath =filepaths[i];
    dirname = '';
    if (filePath.includes("/")) {
      dirname = filePath.substr(0, filePath.lastIndexOf("/") + 1)
    }
    if (!mzxmlDirList.includes(dirname)) {
      mzxmlDirList.push(dirname)
    }
  }
  // In case there is no webkitRelativePath attribute
  if (mzxmlDirList.length == 0) {
    mzxmlDirList.push('')
  }
  return mzxmlDirList
}

function getParentMzxmlDirectories(allMzxmlDirList) {
  let parentMzxmlDirList = []
  for (i=0;i<allMzxmlDirList.length;i++) {
    candidate = allMzxmlDirList[i];
    // If the candidate directory path does not start with any other path in the directory list
    if (!allMzxmlDirList.some(function(dir) {dir !== candidate && candidate.startsWith(dir)})) {
      parentMzxmlDirList.push(candidate)
    }
  }
  return parentMzxmlDirList
}

function getMzxmlsInDirectory(allMzxmlFilePathList, parentDir) {
  let childMzxmls = []
  for (let i = 0; i < allMzxmlFilePathList.length; ++i) {
    filePath = allMzxmlFilePathList[i];
    if (filePath.startsWith(parentDir)) {
      childMzxmls.push(filePath)
    }
  }
  return childMzxmls
}

// TODO:
// 1. When a directory is dropped/selected:
//    1. getAllMzxmlFilePaths
//    2. getAllMzxmlDirectories  // Change this to take a list of strings from step 1
//    3. getParentMzxmlDirectories
//    4. For each parent directory
//       1. Create a form row (duplicating the template form)
//       2. Populate the parent directory in mzxml_dir form elem
//       3. getMzxmlsInDirectory
//       4. Construct string (using .join()) of filepaths - use to populate the mzxml_metadata form elem
// 2. Move the peak annot file inputs into the single form elem inputs and make it a multiple file input (unassociated with sequence metadata)
// 3. Merge the 2 forms (RawDataSubmissionForm and DataSubmissionForm) into 1 form
// 4. Make the Start page accept study doc, annot files, and the study dir... In fact, just take the study dir and find all the files...???!!!
