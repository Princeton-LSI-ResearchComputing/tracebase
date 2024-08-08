// TODO: Figure out how to import methods from other files: browserDownloadExcel, getFileNamesString, DataTransfer
// See https://stackoverflow.com/questions/950087/
document.addEventListener('DOMContentLoaded', function () {
  const peakAnnotInput = document.getElementById('peak_annotation_files_field')
  const peakAnnotList = document.getElementById('pending_peak_annot_files')
  const studyFileContentTag = document.getElementById('output_study_file')
  let currentPeakAnnotFiles = null

  // If there is a study file that was produced
  if (typeof studyFileContentTag !== 'undefined' && studyFileContentTag) {
    browserDownloadExcel('{{ study_filename }}', studyFileContentTag.innerHTML) // eslint-disable-line no-undef
  }

  // Note that the reset button does not trigger a change event, so see its onclick code
  peakAnnotInput.addEventListener(
    'change',
    function () { handlePeakAnnotFiles(peakAnnotInput.files) },
    false
  )

  function showPeakAnnotFiles (files) {
    peakAnnotList.innerHTML = getFileNamesString(files) // eslint-disable-line no-undef
  }

  function handlePeakAnnotFiles (files, add) {
    if (typeof add === 'undefined' || add === null) {
      add = true
    }
    // If we're adding files, oreviously selected files exist, and the field isn't being cleared
    if (add && typeof currentPeakAnnotFiles !== 'undefined' && currentPeakAnnotFiles && files.length > 0) {
      // DataTransfer comes from browsers
      const dT = new DataTransfer() // eslint-disable-line no-undef
      // Add previously added files to the DataTranfer object
      for (let i = 0; i < currentPeakAnnotFiles.length; i++) {
        dT.items.add(currentPeakAnnotFiles[i])
      }
      // Add newly selected files to the DataTranfer object
      for (let i = 0; i < files.length; i++) {
        dT.items.add(files[i])
      }
      // Set the file input element to the combined (previously added and new) files
      peakAnnotInput.files = dT.files
    }
    currentPeakAnnotFiles = peakAnnotInput.files
    showPeakAnnotFiles(peakAnnotInput.files)
  }
})
