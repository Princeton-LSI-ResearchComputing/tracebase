var peakAnnotFormTemplateContainer = null // eslint-disable-line no-var
var peakAnnotFormsTable = null // eslint-disable-line no-var
var peakAnnotDropAreaInput = null // eslint-disable-line no-var
var dataSubmissionForm = null // eslint-disable-line no-var
var singleFormDiv = null // eslint-disable-line no-var
var studyDocInput = null // eslint-disable-line no-var
var annotFilesInput = null // eslint-disable-line no-var
var peakAnnotHash = {} // eslint-disable-line no-var
var annotToMzxmlInput = null // eslint-disable-line no-var
var annotToMzxmlHash = {} // eslint-disable-line no-var

var mzxmlFormTemplateContainer = null // eslint-disable-line no-var
var mzxmlFormsTable = null // eslint-disable-line no-var
var mzxmlDirDropAreaInput = null // eslint-disable-line no-var
var mzxmlSubmissionForm = null // eslint-disable-line no-var
var mzxmlFileListDisplayElem = null // eslint-disable-line no-var
var sequenceDirsHash = {} // eslint-disable-line no-var
var possibleAnnotPaths = {} // eslint-disable-line no-var

/**
 * A method to initialize the peak annotation file form interface.  Dropped files will call addPeakAnnotFileToUpload,
 * and that method needs 2 things that this method initializes:
 * @param {*} templateContainer [tr] A table row containing form input elements for the peak annotation file and the
 * sequence metadata.
 * @param {*} peakAnnotFormsTable [table] The table where the form rows will be added when files are dropped in the drop
 * area.
 */
function initPeakAnnotUploads ( // eslint-disable-line no-unused-vars
  peakAnnotFormTemplateContainer,
  peakAnnotFormsTable,
  peakAnnotDropAreaInput,
  dataSubmissionForm,
  singleFormDiv,
  studyDocInput,
  annotFilesInput,
  annotToMzxmlInput
) {
  globalThis.peakAnnotFormTemplateContainer = peakAnnotFormTemplateContainer;
  globalThis.peakAnnotFormsTable = peakAnnotFormsTable;
  globalThis.peakAnnotDropAreaInput = peakAnnotDropAreaInput;
  globalThis.dataSubmissionForm = dataSubmissionForm;
  globalThis.singleFormDiv = singleFormDiv;
  globalThis.studyDocInput = studyDocInput;
  globalThis.annotFilesInput = annotFilesInput;
  globalThis.annotToMzxmlInput = annotToMzxmlInput;

  // Add an event listener to the study doc input to enable the submit button
  studyDocInput.addEventListener('change', function () {
    enableSubmissionForm();
  });

  // Disable the form submission button to start (because there are no peak annotation form rows yet).
  disableSubmissionForm();
}

function initStudyMetadataUploads ( // eslint-disable-line no-unused-vars
  mzxmlFormTemplateContainer,
  mzxmlFormsTable,
  mzxmlDirDropAreaInput,
  mzxmlSubmissionForm,
  singleFormDiv,
  studyDocInput
) {
  globalThis.mzxmlFormTemplateContainer = mzxmlFormTemplateContainer;
  globalThis.mzxmlFormsTable = mzxmlFormsTable;
  globalThis.mzxmlDirDropAreaInput = mzxmlDirDropAreaInput;
  globalThis.mzxmlSubmissionForm = mzxmlSubmissionForm;
  globalThis.singleFormDiv = singleFormDiv;
  globalThis.studyDocInput = studyDocInput;

  // TODO: Add ability to disable/enable the submit button, meaning, the ability to submit JUST the sequence metadata
  // for autofill of the sequences tab, though that would mean that the feature to select an existing study doc would be
  // required.
}

/**
 * This method takes a single peak annotation file for upload (inside a DataTransfer object) and clones a file upload
 * form for a single file input along with sequence metadata inputs and un-hides the file input.
 * @param {*} file A file object
 */
function addPeakAnnotFileToUpload (file) { // eslint-disable-line no-unused-vars
  // Create a row for the metadata inputs associated with each file
  const newRow = createPeakAnnotFormRow();

  // Clone the form row template, unhide, and set the file name
  makeAnnotFormModifications(file, newRow);

  // Add listeners
  defSeqDirInput = newRow.querySelector("select[name='default_sequence_dir']");
  defScanDirInput = newRow.querySelector("select[name='default_scan_dir']");

  defSeqDirInput.addEventListener('change', function () {
    console.log("CLEARING", file.name, "SCAN SELECT LIST:", defScanDirInput)
    peakAnnotHash[file.name]["defaultScanDirSelectElem"].innerHTML = '';
    peakAnnotHash[file.name]["defaultScanDirs"] = [];
    updateAnnotDefaultScanSelectLists();
    updateAnnotToMzxmlData();
  });

  defScanDirInput.addEventListener('change', function () {
    updateAnnotToMzxmlData();
  });

  // Append the row to the peak annotations form table
  peakAnnotFormsTable.appendChild(newRow);

  return newRow;
}
// TODO: Add the ability to populate the peak annotation files from the dropped study dir.

/**
 * This function clones a tr row containing the form elements for a peak annotation file.
 * @param {*} template - The tr element for the template to clone.
 * @returns the cloned row element containing the cloned form for a peak annot file.
 */
function createPeakAnnotFormRow (template) {
  if (typeof template === 'undefined' || !template) {
    template = peakAnnotFormTemplateContainer;
  }
  return template.cloneNode(true)
}

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
  const totalInput = document.createElement('input');
  totalInput.setAttribute('type', 'hidden');
  totalInput.setAttribute('name', 'form-TOTAL_FORMS');
  totalInput.setAttribute('value', numForms.toString());
  totalInput.setAttribute('id', 'id_form-TOTAL_FORMS');
  dataSubmissionForm.appendChild(totalInput);

  // dataSubmissionForm.innerHTML += '<input type="hidden" name="form-INITIAL_FORMS" value="0" ';
  // dataSubmissionForm.innerHTML += 'id="id_form-INITIAL_FORMS">';
  const initialInput = document.createElement('input');
  initialInput.setAttribute('type', 'hidden');
  initialInput.setAttribute('name', 'form-INITIAL_FORMS');
  initialInput.setAttribute('value', '0');
  initialInput.setAttribute('id', 'id_form-INITIAL_FORMS');
  dataSubmissionForm.appendChild(initialInput);
}

/**
 * This method returns all tr elements contaning a form belonging to the formset we are creating.
 * @returns all tr elements containing an individual (cloned) form.
 */
function getFormRows () {
  return mzxmlFormsTable.querySelectorAll('tr[name="form-set-row"]')
}

/**
 * This function enables the form submission button.
 */
function enableSubmissionForm () {
  const submitInput = document.querySelector('#submit')
  submitInput.removeAttribute('disabled')
}

/**
 * This function disables the form submission button.
 */
function disableSubmissionForm () {
  const submitInput = document.querySelector('#submit')
  submitInput.disabled = true
}

/**
 * This function clears the file picker input element inside the drop area after having created form rows.  It is called
 * from the annot-drop-area code after all dropped/picked files have been processed.  It intentionally leaves the
 * entries in the sequence metadata inputs for re-use upon additional drops/picks.
 * @param {*} newFiles [list of files]: The list of files from a DataTransfer object containing the newly dropped/
 * selected files
 */
function afterAddingPeakAnnotFiles (newFiles) { // eslint-disable-line no-unused-vars
  // This method resets the files in globalThis.annotFilesInput by creating a new DataTransfer object, adding the
  // existing files, then adding the new files.  It also updates globalThis.peakAnnotHash with the newly added files.

  // Add the files to the hidden annotFilesInput
  // See: https://stackoverflow.com/questions/8006715/
  // Create a new DataTransfer object to contain the merged list of files
  const newDT = new DataTransfer();
  let filenames = {};
  let dupes = [];
  let possibleAnnotPath;
  // Add the existing files
  for (i=0;i < globalThis.annotFilesInput.files.length;i++) {
    if (Object.keys(filenames).includes(globalThis.annotFilesInput.files[i].name)) {
      dupes.push(globalThis.annotFilesInput.files[i].name)
    } else {
      annotFile = annotFilesInput.files[i];
      annotName = annotFile.name;
      newDT.items.add(annotFile);

      [studyDir, annotDirPath, annotFilePath] = getFilePath(annotFile);

      possibleAnnotPath = getPossibleAnnotPath(annotName);

      // If there wasn't (somehow) a path (annotDirPath) supplied with the file and the annot file name matches one from
      // the study directory, set the annot file path so that we can auto-select the correct sequence and scan dirs.
      if (
        annotDirPath === ''
        && typeof possibleAnnotPath !== 'undefined'
        && possibleAnnotPath
        && possibleAnnotPath !== 'null'
      ) {
        filenames[annotName] = possibleAnnotPath;
      } else {
        filenames[annotName] = annotDirPath;
      }
    }
  }

  // Add the incoming files
  for (i=0;i < newFiles.length;i++) {
    if (Object.keys(filenames).includes(newFiles[i].name)) {
      dupes.push(newFiles[i].name)
    } else {
      newDT.items.add(newFiles[i]);

      // Update filenames in case the dragged and dropped files include 2 files with the same name
      [studyDir, annotDirPath, annotFilePath] = getFilePath(newFiles[i]);

      possibleAnnotPath = getPossibleAnnotPath(newFiles[i].name);
      console.log("POSSIBLE ANNOT PATH IN SEQ:", possibleAnnotPath, "NONE OF THESE:", possibleAnnotPaths[newFiles[i].name], "WERE IN SEQDIRS:", sequenceDirsHash["allSequenceDirs"]);

      if (
        annotDirPath === ''
        && typeof possibleAnnotPath !== 'undefined'
        && possibleAnnotPath
        && possibleAnnotPath !== 'null'
      ) {
        filenames[newFiles[i].name] = possibleAnnotPath;
        annotFilePath = possibleAnnotPath;
      } else {
        filenames[newFiles[i].name] = annotDirPath;
      }

      let annotFormRow = addPeakAnnotFileToUpload(newFiles[i])

      globalThis.peakAnnotHash[newFiles[i].name] = {
        "annotFilePath": annotFilePath,
        "defaultSequenceDirSelectElem": annotFormRow.querySelector('select[name=default_sequence_dir]'),
        "defaultScanDirSelectElem": annotFormRow.querySelector('select[name=default_scan_dir]'),
        "defaultSequenceDirs": [],
        "defaultScanDirs": [],
      };
    }
  }
  globalThis.annotFilesInput.files = newDT.files;

  // Clear the drop area to accept new files
  peakAnnotDropAreaInput.value = null

  if (dupes.length > 0) {
    alert("Peak annotation filenames must be unique.  Skipped these files with duplicate names:" + dupes)
  }

  // Now that the peak annot form rows have been added, we can populate the select lists and try to make default
  // selections
  updateAnnotDefaultSequenceSelectLists();

  updateAnnotToMzxmlData();

  // Now replace the input element's old files with the merged new files
  // Enable form submission
  enableSubmissionForm();
}

function getPossibleAnnotPath(annotName) {
  // This takes a file name and looks in possibleAnnotPaths to see if a file by that name was seen in 1 sequence
  // directory.  Returns null if not.
  let possibleAnnotPath = null;
  if (
    !Object.keys(sequenceDirsHash).includes("allSequenceDirs")
    || Object.keys(possibleAnnotPaths).length == 0
    || !Object.keys(possibleAnnotPaths).includes(annotName)
  ) {
    return possibleAnnotPath;
  }
  let possibleAnnotPathsInSeqDirs = [];
  // For each possible path
  for (let i = 0; i < possibleAnnotPaths[annotName].length; i++) {
    let possAnnPath = possibleAnnotPaths[annotName][i];
    console.log("COMPARING possAnnPath", possAnnPath);
    // For each sequence directory
    for (let j=0; j < sequenceDirsHash["allSequenceDirs"].length; j++) {
      let seqDirPath = sequenceDirsHash["allSequenceDirs"][j];
      console.log("WITH seqDirPath", seqDirPath);
      // If the possible path is in a sequence directory
      if (possAnnPath.startsWith(seqDirPath + '/') || seqDirPath === '') {
        console.log("GOT ONE", possAnnPath, "IS IN", seqDirPath);
        // Add it to the possible matching paths
        possibleAnnotPathsInSeqDirs.push(possAnnPath);
        break;
      }
    }
  }
  if (possibleAnnotPathsInSeqDirs.length === 1) {
    possibleAnnotPath = possibleAnnotPathsInSeqDirs[0];
  }
  return possibleAnnotPath;
}

function getFilePath(fileObject) {
  let tmpPath;

  // Extract the relative path of the file (not including the file name)
  if (
    (Object.hasOwn(fileObject, 'webkitRelativePath') || 'webkitRelativePath' in fileObject)
    && typeof fileObject.webkitRelativePath !== 'undefined'
    && fileObject.webkitRelativePath
    && fileObject.webkitRelativePath !== ''
  ) {
    tmpPath = fileObject.webkitRelativePath;
  } else if (
    (Object.hasOwn(fileObject, 'path') || 'path' in fileObject)
    && typeof fileObject.path !== 'undefined'
    && fileObject.path
    && fileObject.path !== ''
  ) {
    // Paths should be relative to the dropped directory, but the code that assigns the apth attribute includes the
    // dropped directory in the path, so here, we trim it off:
    tmpPath = fileObject.path
  } else if (
    (Object.hasOwn(fileObject, 'fullpath') || 'fullpath' in fileObject)
    && typeof fileObject.fullpath !== 'undefined'
    && fileObject.fullpath
    && fileObject.fullpath !== ''
  ) {
    tmpPath = fileObject.fullpath;
  } else {
    console.log("Getting name for file", fileObject)
    tmpPath = fileObject.name
  }
  // Assumes always a relative path.  The dir walk done in the drop area code yields relative paths that start with
  // a slash, as if it was an absolute path, but the webkitRelativePath never prepends that slash.
  if (tmpPath.startsWith("/")) {
    tmpPath = tmpPath.replace(/^\//, '');
  }
  // Both methods include the selected/dropped directory in the path, but we want paths that are relative to that
  // directory, so we chop off the study directory.
  let [root, ...filePathList] = tmpPath.split('/');

  // Determine the relative directory path
  let tmpDirPathList = filePathList.slice();
  if (tmpDirPathList.length > 1) {
    tmpDirPathList.pop();
    dirPath = tmpDirPathList.join('/');
  } else {
    dirPath = '';
  }

  return [root, dirPath, filePathList.join('/')];
}

function updateAnnotDefaultSequenceSelectLists() {
  // Return if either the sequenceDirsHash or peakAnnotHash are empty
  if (Object.keys(sequenceDirsHash).length === 0 || Object.keys(peakAnnotHash).length === 0) {
    console.log("There is either no study folder added or not peak annotation files added.");
    return;
  }

  // For annotFileName in peakAnnotHash
  for (const annotFileName in peakAnnotHash) {
    // Get the input defaultSequenceDir select list input element
    console.log("Object.keys(peakAnnotHash)", Object.keys(peakAnnotHash), "peakAnnotHash", peakAnnotHash, "annotFileName", annotFileName, "peakAnnotHash", peakAnnotHash)
    annotSeqDirSelectElem = peakAnnotHash[annotFileName]["defaultSequenceDirSelectElem"];
    annotScanDirSelectElem = peakAnnotHash[annotFileName]["defaultScanDirSelectElem"];
    annotFilePath = peakAnnotHash[annotFileName]["annotFilePath"];

    // If peakAnnotHash[annotFileName]["defaultSequenceDirs"] is empty
    if (peakAnnotHash[annotFileName]["defaultSequenceDirs"].length === 0) {
      // Populate html options from the sequenceDirsHash
      annotSeqDirSelectElem.innerHTML = '';
      sequenceDirsHash["sequenceSelectOptions"].forEach(option => {annotSeqDirSelectElem.appendChild(option.cloneNode(true));});
      // Populate peakAnnotHash[annotFileName]["defaultSequenceDirs"] from the sequenceDirsHash
      console.log("ADDED ALL SEQDIRS TO", annotFileName, "EMPTY annotSeqDirSelectElem", annotSeqDirSelectElem)
      globalThis.peakAnnotHash[annotFileName]["defaultSequenceDirs"] = sequenceDirsHash["allSequenceDirs"].slice();
      // The default sequence directory select list changed, so empty the default scan directory select list
      annotScanDirSelectElem.innerHTML = '';
      peakAnnotHash[annotFileName]["defaultScanDirs"] = [];
    }
    // Else If peakAnnotHash[annotFileName]["defaultSequenceDirs"] differs from the sequenceDirsHash
    else if (!arraysEqual(peakAnnotHash[annotFileName]["defaultSequenceDirs"], sequenceDirsHash["allSequenceDirs"])) {
      // Save the current selected option
      savedDefSeqVal = annotSeqDirSelectElem.value;

      // Empty the innerHTML and peakAnnotHash[annotFileName]["defaultSequenceDirs"]
      // Populate innerHTML and peakAnnotHash[annotFileName]["defaultSequenceDirs"] from the sequenceDirsHash
      annotSeqDirSelectElem.innerHTML = '';
      sequenceDirsHash["sequenceSelectOptions"].forEach(option => {annotSeqDirSelectElem.appendChild(option.cloneNode(true));});
      globalThis.peakAnnotHash[annotFileName]["defaultSequenceDirs"] = sequenceDirsHash["allSequenceDirs"].slice();

      // If the saved selected option exists among the new options, select it
      if (
        typeof savedDefSeqVal !== 'undefined'
        && savedDefSeqVal
        && peakAnnotHash[annotFileName]["defaultSequenceDirs"].includes(savedDefSeqVal)
      ) {
        console.log("annotFilePath", annotFilePath, "HAD A PREVIOUS SELECTION:", savedDefSeqVal)
        annotSeqDirSelectElem.value = savedDefSeqVal;
      }
      console.log("ADDED ALL SEQDIRS TO", annotFileName, "DIFFERING annotSeqDirSelectElem", annotSeqDirSelectElem);
      // The default sequence directory select list changed, so empty the default scan directory select list
      annotScanDirSelectElem.innerHTML = '';
      peakAnnotHash[annotFileName]["defaultScanDirs"] = [];
    } else {
      console.log("Apparently these arrays are equal:", peakAnnotHash[annotFileName]["defaultSequenceDirs"], sequenceDirsHash["allSequenceDirs"])
    }

    // If there is not a defaultSequenceDir select list selection
    if (typeof annotSeqDirSelectElem.value === 'undefined' || !annotSeqDirSelectElem.value || annotSeqDirSelectElem.value === 'null') {
      const annotInSeqDir = (seqDir) => annotFilePath.startsWith(seqDir + '/') || seqDir == '';
      matchingSeqDirs = sequenceDirsHash["allSequenceDirs"].filter(annotInSeqDir);
      // If an unambiguous defaultSequenceDir selection can be made
      if (matchingSeqDirs.length === 1) {
        // Set the selection
        console.log("annotFilePath", annotFilePath, "MATCHED 1 SEQDIR:", matchingSeqDirs[0])
        annotSeqDirSelectElem.value = matchingSeqDirs[0];
      } else {
        console.log("annotFilePath", annotFilePath, "MATCHED", matchingSeqDirs.length, "SEQDIR:", matchingSeqDirs, "ALL SEQDIRS:", sequenceDirsHash["allSequenceDirs"])
      }
    }
  }

  updateAnnotDefaultScanSelectLists();
}

function updateAnnotDefaultScanSelectLists() {
  // TODO: FIX BUG: DO NOT SELECT A SCAN DIRECTORY IF THERE ARE MULTIPLE TO CHOOSE FROM AND THE ANNOTATION FILE IS OUTSIDE ANY SEQUENCE DIRECTORY

  // Return if either the sequenceDirsHash or peakAnnotHash are empty
  if (Object.keys(sequenceDirsHash).length === 0 || Object.keys(peakAnnotHash).length === 0) {
    console.log("There is either no study folder added or not peak annotation files added.");
    return;
  }

  // For annotFileName in peakAnnotHash
  for (const annotFileName in peakAnnotHash) {
    // Get the input defaultSequenceDir select list input element
    console.log("Object.keys(peakAnnotHash)", Object.keys(peakAnnotHash), "peakAnnotHash", peakAnnotHash, "annotFileName", annotFileName, "peakAnnotHash", peakAnnotHash)
    annotSeqDirSelectElem = peakAnnotHash[annotFileName]["defaultSequenceDirSelectElem"];
    annotScanDirSelectElem = peakAnnotHash[annotFileName]["defaultScanDirSelectElem"];
    annotFilePath = peakAnnotHash[annotFileName]["annotFilePath"];

    const annotInScanDir = (scanDir) => annotFilePath.startsWith(scanDir + '/') || scanDir == '';

    // If there is a defaultSequenceDir select list selection (check anew)
    if (typeof annotSeqDirSelectElem.value !== 'undefined' && ((annotSeqDirSelectElem.value && annotSeqDirSelectElem.value !== 'null') || annotSeqDirSelectElem.value === '')) {
      seqdirpath = annotSeqDirSelectElem.value;
      annotDefScanDirs = peakAnnotHash[annotFileName]["defaultScanDirs"];
      // If peakAnnotHash[annotFileName]["defaultScanDirs"] is empty
      if (typeof annotDefScanDirs === 'undefined' || !annotDefScanDirs || annotDefScanDirs.length === 0) {
        // Populate innerHTML from the sequenceDirsHash
        annotScanDirSelectElem.innerHTML = '';
        console.log('seqdirpath', seqdirpath, 'sequenceDirsHash["allScanDirs"]', sequenceDirsHash["allScanDirs"])
        sequenceDirsHash["allScanDirs"][seqdirpath]["scanSelectOptions"].forEach(
          option => {annotScanDirSelectElem.appendChild(option.cloneNode(true));
        });
        // Populate peakAnnotHash[annotFileName]["defaultScanDirs"] from the sequenceDirsHash
        peakAnnotHash[annotFileName]["defaultScanDirs"] = sequenceDirsHash["allScanDirs"][seqdirpath]["scanDirs"];
        console.log("ADDED ALL SCANDIRS TO", annotFileName, "EMPTY annotScanDirSelectElem", annotScanDirSelectElem)
      }
      // Else If peakAnnotHash[annotFileName]["defaultScanDirs"] differs from the sequenceDirsHash
      else if (
        !arraysEqual(
          peakAnnotHash[annotFileName]["defaultScanDirs"],
          sequenceDirsHash["allScanDirs"][seqdirpath]["scanDirs"]
        )
      ) {
        // Save the current selected option
        savedDefScanVal = annotScanDirSelectElem.value;
        // Empty innerHTML and peakAnnotHash[annotFileName]["defaultScanDirs"]
        // Populate innerHTML from the sequenceDirsHash
        annotScanDirSelectElem.innerHTML = '';
        sequenceDirsHash["allScanDirs"][seqdirpath]["scanSelectOptions"].forEach(
          option => {annotScanDirSelectElem.appendChild(option.cloneNode(true));
        });
        // Populate peakAnnotHash[annotFileName]["defaultScanDirs"]) from the sequenceDirsHash
        peakAnnotHash[annotFileName]["defaultScanDirs"] = sequenceDirsHash["allScanDirs"][seqdirpath]["scanDirs"];
        if (typeof savedDefScanVal !== 'undefined' && savedDefScanVal) {
          // If the saved selected option exists among the new options
          matchingScanDirs = peakAnnotHash[annotFileName]["defaultScanDirs"].filter(annotInScanDir);
          if (matchingScanDirs.length > 0) {
            // Select it
            annotScanDirSelectElem.value = savedDefScanVal;
          }
        }
        console.log("ADDED ALL SCANDIRS TO", annotFileName, "DIFFERING annotScanDirSelectElem", annotScanDirSelectElem)
      } else {
        console.log("Apparently these arrays are equal:", peakAnnotHash[annotFileName]["defaultScanDirs"], sequenceDirsHash["allScanDirs"][seqdirpath]["scanDirs"])
      }
      // If there is not a defaultScanDir select list selection
      if (typeof annotScanDirSelectElem.value === 'undefined' || !annotScanDirSelectElem.value || annotScanDirSelectElem.value === 'null') {
        // If an unambiguous defaultScanDir selection can be made
        matchingScanDirs = peakAnnotHash[annotFileName]["defaultScanDirs"].filter(annotInScanDir);
        if (matchingScanDirs.length === 1) {
          // Set the selection
          annotScanDirSelectElem.value = matchingScanDirs[0];
        } else {
          console.log("annotFilePath", annotFilePath, "MATCHED", matchingScanDirs.length, "SCANDIR:", matchingScanDirs, "ALL SCANDIRS:", peakAnnotHash[annotFileName]["defaultScanDirs"])
        }
      } else {
        console.log("annotScanDirSelectElem.value:", annotScanDirSelectElem.value);
      }
    // Else
    } else {
      // Empty the innerHTML and peakAnnotHash[annotFileName]["defaultScanDirs"]
      annotScanDirSelectElem.innerHTML = '';
      peakAnnotHash[annotFileName]["defaultScanDirs"] = [];
      console.log("EMPTIED SCANDIRS FOR", annotFileName, "BECAUSE THE DEFSEQDIRS SELECT LIST HAS NO SELECTION")
    }
  }
}

function updateAnnotToMzxmlData() {
  // Start from scratch
  globalThis.annotToMzxmlHash = {};
  for (annotFileName in peakAnnotHash) {
    globalThis.annotToMzxmlHash[annotFileName] = {
      "annotFilePath": peakAnnotHash[annotFileName]["annotFilePath"],
      "defaultSequenceDir": peakAnnotHash[annotFileName]["defaultSequenceDirSelectElem"].value,
      "defaultScanDir": peakAnnotHash[annotFileName]["defaultScanDirSelectElem"].value
    };
  }
  globalThis.annotToMzxmlInput.value = JSON.stringify(annotToMzxmlHash);
}

// See: https://stackoverflow.com/a/16436975/2057516
function arraysEqual(a, b) {
  if (a === b) return true;
  if (a == null || b == null) return false;
  if (a.length !== b.length) return false;

  // If you don't care about the order of the elements inside
  // the array, you should sort both arrays here.
  // Please note that calling sort on an array will modify that array.
  // you might want to clone your array first.

  for (var i = 0; i < a.length; ++i) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

function refreshStudyMetadata (files) { // eslint-disable-line no-unused-vars
  let [studyDirs, filepaths, possibleStudyDocs, possiblePeakAnnotFiles] = getStudyMetadata(files)

  // Save the possible peak annot paths to help automatically select the default sequence dir and default scan dir based
  // on colocation with the saved sequence dirs and their scan dirs
  globalThis.possibleAnnotPaths = possiblePeakAnnotFiles;

  // Only allow 1 folder: 1 study folder or 1 submission folder (with multiple studies).
  if (studyDirs.length > 1) {
    alert(
      'Only 1 study folder allowed.  '
      + studyDirs.length
      + ' is too many: ["'
      + studyDirs.join('", "')
      + '"].\n\n'
      + 'If you have multiple studies in this submission, put both study folders in a single submission folder and '
      + 'select/drop that folder.'
    );
    return
  }

  let studyDir = studyDirs[0];

  console.log("filepaths:", filepaths)

  noSeqDirOption = document.createElement("option");
  noSeqDirOption.value = null;
  noSeqDirOption.text = " --- Select a sequence folder --- ";
  noSeqDirOption.selected = true;

  noScanDirOption = document.createElement("option");
  noScanDirOption.value = null;
  noScanDirOption.text = " --- Select a scan sub-folder --- ";
  noScanDirOption.selected = true;

  // Clear out previously added sequence form rows and saved sequence directories
  mzxmlFormsTable.innerHTML = ''
  globalThis.sequenceDirsHash = {
    "allSequenceDirs": [],
    "sequenceSelectOptions": [noSeqDirOption.cloneNode(true)],
    "allScanDirs": {}
  }

  // Update the mzxml_file_list form element
  mzxmlListInput = document.getElementById('mzxml_file_list_input_id')
  mzxmlListInput.value = JSON.stringify(filepaths)

  let mzxmldirpaths = getAllMzxmlDirectories(filepaths)
  let sequenceDirPathsHash = getParentMzxmlDirectories(mzxmldirpaths)

  // For each sequence directory
  for (seqdirpath of Object.keys(sequenceDirPathsHash)) {
    // Create a form row (duplicating the template) and populate the parent directory in default_sequence_dir form elem
    addSequenceFormRow(seqdirpath);

    globalThis.sequenceDirsHash["allSequenceDirs"].push(seqdirpath);

    // Add this sequence directory to the template sequence select list options
    var sequenceOption = document.createElement("option");
    sequenceOption.value = seqdirpath;
    sequenceOption.text = seqdirpath;
    if (seqdirpath === '') {
      sequenceOption.text += ". (" + studyDir + ")";
    }
    globalThis.sequenceDirsHash["sequenceSelectOptions"].push(sequenceOption);
    console.log("ADDED SEQDIR OPTION sequenceOption", sequenceOption)

    // Add this scan subdirectories to the template scan directory select list options
    globalThis.sequenceDirsHash["allScanDirs"][seqdirpath] = {
      "scanDirs": [],
      "scanSelectOptions": [noScanDirOption.cloneNode(true)]
    };
    for (var i = 0; i < sequenceDirPathsHash[seqdirpath].length; i++) {
      scanSubDir = sequenceDirPathsHash[seqdirpath][i];
      var scanSubDirOption = document.createElement("option");
      scanSubDirOption.value = scanSubDir;
      scanSubDirOption.text = scanSubDir;
      if (scanSubDir === '') {
        if (seqdirpath === '') {
          scanSubDirOption.text += ". (" + studyDir + ")";
        } else {
          scanSubDirOption.text += ". (" + seqdirpath + ")";
        }
      }
      globalThis.sequenceDirsHash["allScanDirs"][seqdirpath]["scanDirs"].push(scanSubDir);
      globalThis.sequenceDirsHash["allScanDirs"][seqdirpath]["scanSelectOptions"].push(scanSubDirOption);
    }
  }

  // Now that the peak annot form rows have been added, we can populate the select lists and try to make default
  // selections
  updateAnnotDefaultSequenceSelectLists();

  // TODO: Populate a list of files for each sequence form row that only display when the user does something (like click a caret or hover or something)
}

function addSequenceFormRow (seqdirpath) {
  let newRow = mzxmlFormTemplateContainer.cloneNode(true);
  makeSequenceFormModifications(newRow, seqdirpath);
  mzxmlFormsTable.appendChild(newRow);
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
  if (seqdirpath === '') {
    seqDirInput.placeholder = 'study directory contains data for a single sequence';
  }
  // seqDirInput.readonly = true
  seqDirInput.setAttribute('readonly', true)

  console.log("Updated seqdir input:", seqDirInput, "seqdir:", seqdirpath)

  // Remove the ID (which is what is used to identify the row template)
  formRow.removeAttribute('id')
}

/**
 * This function clears all of the previously added peak annotation form rows.
 */
function clearSubmissionForm () { // eslint-disable-line no-unused-vars
  peakAnnotFormsTable.innerHTML = '';
  mzxmlFormsTable.innerHTML = '';
  globalThis.peakAnnotHash = {};
  globalThis.sequenceDirsHash = {};
  globalThis.possibleAnnotPaths = {};
  globalThis.annotToMzxmlHash = {};
  dataSubmissionForm.reset();
  // TODO: Clear each peak annotation files' select lists (defaultSequenceDir and defaultScanDir)
  disableSubmissionForm();
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

function getStudyMetadata(files) {
  // See: https://stackoverflow.com/a/60538623/2057516
  let allMzxmlFilePathList = [];
  let studyDirs = [];
  let possibleStudyDocs = [];
  let possiblePeakAnnotFiles = {};

  // This method can take some time, and things won't work until it's done, so...
  // Obtain the divs for graying out the page while dropped items are read
  overlayDiv = document.getElementById("overlay");
  popupDiv = document.getElementById("popup");
  popupDiv.innerHTML = 'Processing files.  Please wait.';

  console.log("graying out page");

  // const start = Date.now();

  overlayDiv.style.display = "block";
  popupDiv.style.display = "block";

  for (let i = 0; i < files.length; ++i) {
    let fileName = files[i].name;

    console.log("Checking out file", fileName)

    // Extract the relative path of the file (not including the file name)
    let [studyDir, dirPath, filePath] = getFilePath(files[i]);

    if (!studyDirs.includes(studyDir)) {
      studyDirs.push(studyDir);
    }

    if (fileName.toLowerCase().endsWith(".mzxml")) {
      allMzxmlFilePathList.push(filePath)
    } else if (
      fileName.toLowerCase().endsWith(".xlsx")
      || fileName.toLowerCase().endsWith(".csv")
      || fileName.toLowerCase().endsWith(".tsv")
    ) {
      // The study doc must be in the root directory
      if (fileName.toLowerCase().endsWith(".xlsx") && dirPath === "") {
        possibleStudyDocs.push(fileName)
      }

      // There can be multiple files with the same name, especially if original versions of files are saved in a
      // subdirectory, so this accounts for all versions by storing an array of file paths keyed on filename.
      if (!Object.keys(possiblePeakAnnotFiles).includes(fileName)) {
        possiblePeakAnnotFiles[fileName] = [];
      }

      if (dirPath === '') {
        possiblePeakAnnotFiles[fileName].push(fileName);
      } else {
        possiblePeakAnnotFiles[fileName].push(filePath);
      }
    }
  }
  console.log("Done reading folder")

  // let elapsed = Date.now() - start;
  // while (elapsed < 3000) {
  //   if (elapsed % 1000 === 0) {console.log(elapsed)}
  //   elapsed = Date.now() - start;
  // }
  console.log("Un-graying page");

  // Hide the overlay and popup when the task is done
  overlayDiv.style.display = "none";
  popupDiv.style.display = "none";

  return [studyDirs, allMzxmlFilePathList, possibleStudyDocs, possiblePeakAnnotFiles]
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
      dirname = filePath.substr(0, filePath.lastIndexOf("/"))
    }
    if (!mzxmlDirList.includes(dirname)) {
      mzxmlDirList.push(dirname)
    }
  }
  // In case there is no webkitRelativePath attribute
  if (mzxmlDirList.length === 0) {
    mzxmlDirList.push('')
  }
  console.log("mzXML dirs:", mzxmlDirList)
  return mzxmlDirList
}

function getParentMzxmlDirectories(allMzxmlDirList) {

  let candidate
  const isasubdir = (dir) => dir !== candidate && candidate.startsWith(dir);
  // Get all of the parent directories
  let parentMzxmlDirList = []
  for (i=0; i < allMzxmlDirList.length; i++) {
    candidate = allMzxmlDirList[i];
    // If the candidate directory path does not start with any other path in the directory list
    if (!allMzxmlDirList.some(isasubdir)) {
      console.log("Candidate:", candidate, "does not start with any of:", allMzxmlDirList)
      parentMzxmlDirList.push(candidate)
    }
  }
  console.log("mzXML parent dirs:", parentMzxmlDirList)

  // Now build a hash of the parent directories and their scan subdirectories
  let parentMzxmlDirHash = {}
  // Initialize empty lists
  for (parentDir of parentMzxmlDirList) {
    parentMzxmlDirHash[parentDir] = [];
  }
  // Add the scan subdirectories directories to the arrays
  for (i=0; i < allMzxmlDirList.length; i++) {
    candidate = allMzxmlDirList[i];
    // If the candidate directory path starts with any other path in the directory list
    if (allMzxmlDirList.some(isasubdir)) {
      for (parentDir of parentMzxmlDirList) {
        if (candidate.startsWith(parentDir)) {
          subDir = candidate.replace(parentDir, '');
          if (subDir.startsWith('/')) {
            subDir = subDir.replace('/', '')
          }
          console.log("subDir:", subDir, "belongs to parentDir:", parentDir)
          parentMzxmlDirHash[parentDir].push(subDir)
        }
      }
    } else {
      // Every parentDir can be its own scan directory
      parentMzxmlDirHash[candidate].push('')
    }
  }
  console.log("parentMzxmlDirHash:", parentMzxmlDirHash)
  return parentMzxmlDirHash
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
