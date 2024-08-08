document.addEventListener('DOMContentLoaded', function () {
  peak_annot_input = document.getElementById('peak_annotation_files_field')
  peak_annot_list = document.getElementById('pending_peak_annot_files')
  study_file_content_tag = document.getElementById('output_study_file')
  current_peak_annot_files = null

  // If there is a study file that was produced
  if (typeof study_file_content_tag !== 'undefined' && study_file_content_tag) {
    browserDownloadExcel('{{ study_filename }}', study_file_content_tag.innerHTML)
  }

  // Note that the reset button does not trigger a change event, so see its onclick code
  peak_annot_input.addEventListener(
    'change',
    function () { handlePeakAnnotFiles(peak_annot_input.files) },
    false
  )

  function handlePeakAnnotFiles (files, add) {
    if (typeof add === 'undefined' || add === null) {
      add = true
    }
    // If we're adding files, oreviously selected files exist, and the field isn't being cleared
    if (add && typeof current_peak_annot_files !== 'undefined' && current_peak_annot_files && files.length > 0) {
      const dT = new DataTransfer()
      // Add previously added files to the DataTranfer object
      for (let i = 0; i < current_peak_annot_files.length; i++) {
        dT.items.add(current_peak_annot_files[i])
      }
      // Add newly selected files to the DataTranfer object
      for (let i = 0; i < files.length; i++) {
        dT.items.add(files[i])
      }
      // Set the file input element to the combined (previously added and new) files
      peak_annot_input.files = dT.files
    }
    current_peak_annot_files = peak_annot_input.files
    showPeakAnnotFiles(peak_annot_input.files)
  }
})

function showPeakAnnotFiles (files) {
  peak_annot_list.innerHTML = getFileNamesString(files)
}
