var templateContainer = null // eslint-disable-line no-var
var peakAnnotFormsTable = null // eslint-disable-line no-var
var dropAreaInput = null // eslint-disable-line no-var

/**
 * A method to initialize the peak annotation file form interface.  Dropped files will call addPeakAnnotFileToUpload,
 * and that method needs 2 things that this method initializes:
 * @param {*} templateContainer [tr] A table row containing form input elements for the peak annotation file and the
 * sequence metadata.
 * @param {*} peakAnnotFormsTable [table] The table where the form rows will be added when files are dropped in the drop
 * area.
 */
function initPeakAnnotUploads(templateContainer, peakAnnotFormsTable, dropAreaInput) {
    globalThis.templateContainer = templateContainer;
    globalThis.peakAnnotFormsTable = peakAnnotFormsTable;
    globalThis.dropAreaInput = dropAreaInput;
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

function createPeakAnnotFormRow(template) {
  if (typeof template === 'undefined' || !template) {
      template = templateContainer;
    }
    return template.cloneNode(true);
}

function makeFormModifications(dT, formRow) {
  fileTd = formRow.querySelector('#fileColumn');
  fileTd.style = null;
  fileInput = formRow.querySelector('input[name="peak_annotation_file"]');
  fileInput.files = dT.files;
}

/**
 * This function clears the file picker input element inside the drop area after having created form rows.  It is called
 * from the drop-area code after all dropped/picked files have been processed.  It intentionally leaves the entries in
 * the sequence metadata inputs for re-use upon additional drops/picks.
 */
function afterAddingFiles() {
  globalThis.dropAreaInput.value = null;
}

function clearPeakAnnotFiles() {
  peakAnnotFormsTable.innerHTML = '';
}
