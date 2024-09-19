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
function initPeakAnnotUploads (templateContainer, peakAnnotFormsTable, dropAreaInput, dataSubmissionForm) { // eslint-disable-line no-unused-vars
  globalThis.templateContainer = templateContainer
  globalThis.peakAnnotFormsTable = peakAnnotFormsTable
  globalThis.dropAreaInput = dropAreaInput
  globalThis.dataSubmissionForm = dataSubmissionForm
  // Disable the form submission button to start (because there are no peak annotation form rows yet.
  disablePeakAnnotForm()
}

/**
 * This method takes a single peak annotation file for upload (inside a DataTransfer object) and clones a file upload
 * form for a single file input along with sequence metadata inputs and un-hides the file input.
 * @param {*} dT [DataTransfer]: A DataTransfer object containing a single file for upload
 */
function addPeakAnnotFileToUpload (dT, template) { // eslint-disable-line no-unused-vars
  const newRow = createPeakAnnotFormRow(template)
  makeFormModifications(dT, newRow)
  peakAnnotFormsTable.appendChild(newRow)
}

/**
 * This function clones a tr row containing the form elements for a peak annotation file.
 * @param {*} template - The tr element for the template to clone.
 * @returns the cloned row element containing the cloned form for a peak annot file.
 */
function createPeakAnnotFormRow (template) {
  if (typeof template === 'undefined' || !template) {
    template = templateContainer
  }
  return template.cloneNode(true)
}

/**
 * Un-hide the file input column and set the files of the file input.
 * @param {*} dT - DataTransfer object containing 1 file in its files attribute.
 * @param {*} formRow - The row element containing the form.
 */
function makeFormModifications (dT, formRow) {
  // Un-hide the file column
  const fileTd = formRow.querySelector('#fileColumn')
  fileTd.style = null
  // Set the file for the file input
  const fileInput = formRow.querySelector('input[name="peak_annotation_file"]')
  fileInput.files = dT.files
  // Remove the ID (which is what is used to identify the row template)
  formRow.removeAttribute('id')
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
  const formRows = getFormRows()
  for (let r = 0; r < formRows.length; r++) {
    const formRow = formRows[r]
    const inputElems = formRow.querySelectorAll('input')
    // Prepend attributes 'id', 'name', and 'for' with "form-0-", as is what django expects from a formset
    const prefix = 'form-' + r.toString() + '-'
    for (let i = 0; i < inputElems.length; i++) {
      const inputElem = inputElems[i]
      // If this input element contains the form-control class (i.e. we're using the presence of the form-control class
      // to infer that the element is an explicitly added input element (not some shadow element)
      if (inputElem.classList.contains('form-control')) {
        if (inputElem.for) {
          inputElem.for = prefix + inputElem.for
        }
        if (inputElem.id) {
          inputElem.id = prefix + inputElem.id
        }
        if (inputElem.name) {
          inputElem.name = prefix + inputElem.name
        }
      }
    }
  }
  return formRows.length
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
  return peakAnnotFormsTable.querySelectorAll('tr[name="drop-annot-metadata-row"]')
}

/**
 * This function enables the form submission button.
 */
function enablePeakAnnotForm () {
  const submitInput = dataSubmissionForm.querySelector('#submit')
  submitInput.removeAttribute('disabled')
}

/**
 * This function disables the form submission button.
 */
function disablePeakAnnotForm () {
  const submitInput = dataSubmissionForm.querySelector('#submit')
  submitInput.disabled = true
}

/**
 * This function clears the file picker input element inside the drop area after having created form rows.  It is called
 * from the drop-area code after all dropped/picked files have been processed.  It intentionally leaves the entries in
 * the sequence metadata inputs for re-use upon additional drops/picks.
 */
function afterAddingFiles () { // eslint-disable-line no-unused-vars
  dropAreaInput.value = null
  enablePeakAnnotForm()
}

/**
 * This function clears all of the previously added peak annotation form rows.
 */
function clearPeakAnnotFiles () { // eslint-disable-line no-unused-vars
  // tableElems = peakAnnotFormsTable.getElementsByTagName("*");
  // for (let i = 0; i < tableElems.length; ++i) {
  //   tableElems[i].remove();
  // }
  peakAnnotFormsTable.innerHTML = ''
}
