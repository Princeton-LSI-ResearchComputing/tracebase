var templateContainer = null // eslint-disable-line no-var
var peakAnnotFormsTable = null // eslint-disable-line no-var
var dropAreaInput = null // eslint-disable-line no-var
var dataSubmissionForm = null // eslint-disable-line no-var

/**
 * A method to initialize the peak annotation file form interface.  Dropped files will call addPeakAnnotFileToUpload,
 * and that method needs 2 things that this method initializes:
 * @param {*} templateContainer [tr] A table row containing form input elements for the peak annotation file and the
 * sequence metadata.
 * @param {*} peakAnnotFormsTable [table] The table where the form rows will be added when files are dropped in the drop
 * area.
 */
function initPeakAnnotUploads(templateContainer, peakAnnotFormsTable, dropAreaInput, dataSubmissionForm) {
    globalThis.templateContainer = templateContainer;
    globalThis.peakAnnotFormsTable = peakAnnotFormsTable;
    globalThis.dropAreaInput = dropAreaInput;
    globalThis.dataSubmissionForm = dataSubmissionForm;
    // Disable the form submission button to start (because there are no peak annotation form rows yet.
    disablePeakAnnotForm();
}

/**
 * This method takes a single peak annotation file for upload (inside a DataTransfer object) and clones a file upload
 * form for a single file input along with sequence metadata inputs and un-hides the file input.
 * @param {*} dT [DataTransfer]: A DataTransfer object containing a single file for upload
 */
function addPeakAnnotFileToUpload(dT, template) { // eslint-disable-line no-var
  var newRow = createPeakAnnotFormRow(template);
  makeFormModifications(dT, newRow);
  peakAnnotFormsTable.appendChild(newRow);
}

/**
 * This function clones a tr row containing the form elements for a peak annotation file.
 * @param {*} template - The tr element for the template to clone.
 * @returns the cloned row element containing the cloned form for a peak annot file.
 */
function createPeakAnnotFormRow(template) {
  if (typeof template === 'undefined' || !template) {
      template = templateContainer;
    }
    return template.cloneNode(true);
}

/**
 * Un-hide the file input column and set the files of the file input.
 * @param {*} dT - DataTransfer object containing 1 file in its files attribute.
 * @param {*} formRow - The row element containing the form.
 */
function makeFormModifications(dT, formRow) {
  fileTd = formRow.querySelector('#fileColumn');
  fileTd.style = null;
  fileInput = formRow.querySelector('input[name="peak_annotation_file"]');
  fileInput.files = dT.files;
}

/**
 * This function enables the form submission button.
 */
function enablePeakAnnotForm() {
  submitInput = dataSubmissionForm.querySelector('#submit');
  submitInput.removeAttribute('disabled');
}

/**
 * This function disables the form submission button.
 */
function disablePeakAnnotForm() {
  submitInput = dataSubmissionForm.querySelector('#submit');
  submitInput.disabled = true;
}

/**
 * This function clears the file picker input element inside the drop area after having created form rows.  It is called
 * from the drop-area code after all dropped/picked files have been processed.  It intentionally leaves the entries in
 * the sequence metadata inputs for re-use upon additional drops/picks.
 */
function afterAddingFiles() {
  dropAreaInput.value = null;
  enablePeakAnnotForm();
}

/**
 * This function clears all of the previously added peak annotation form rows.
 */
function clearPeakAnnotFiles() {
  peakAnnotFormsTable.innerHTML = '';
}
