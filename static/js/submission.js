// TODO: Figure out how to import methods from other files: browserDownloadExcel
// See https://stackoverflow.com/questions/950087/
document.addEventListener('DOMContentLoaded', function () {
  const studyFileContentTag = document.getElementById('output_study_file')
  const studyFileNameTag = document.getElementById('output_study_file_name')

  // If there is a study file that was produced
  if (typeof studyFileContentTag !== 'undefined' && studyFileContentTag) {
    browserDownloadExcel( // eslint-disable-line no-undef
      studyFileNameTag.innerHTML,
      studyFileContentTag.innerHTML
    )
  }
})
