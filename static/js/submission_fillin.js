function initListeners (minusPath, plusPath) { // eslint-disable-line no-unused-vars
  const studydetails = document.getElementById('study-details')
  const studydetailsbtn = document.getElementById('study-details-button')
  studydetails.addEventListener('hide.bs.collapse', function (e) {
    e.stopPropagation()
    studydetailsbtn.src = plusPath
    setCookie('study-details-shown', 'false')
  })
  studydetails.addEventListener('show.bs.collapse', function (e) {
    e.stopPropagation()
    studydetailsbtn.src = minusPath
    setCookie('study-details-shown', 'true')
  })
  let shown = getCookie('study-details-shown')
  if (shown === 'true') { studydetailsbtn.click() }

  const tracersdetails = document.getElementById('tracers-details')
  const tracersdetailsbtn = document.getElementById('tracers-details-button')
  tracersdetails.addEventListener('hide.bs.collapse', function (e) {
    e.stopPropagation()
    tracersdetailsbtn.src = plusPath
    setCookie('tracers-details-shown', 'false')
  })
  tracersdetails.addEventListener('show.bs.collapse', function (e) {
    e.stopPropagation()
    tracersdetailsbtn.src = minusPath
    setCookie('tracers-details-shown', 'true')
  })
  shown = getCookie('tracers-details-shown')
  if (shown === 'true') { tracersdetailsbtn.click() }

  const addtracersdetails = document.getElementById('add-tracers-details')
  const addtracersdetailsbtn = document.getElementById('add-tracers-details-button')
  addtracersdetails.addEventListener('hide.bs.collapse', function (e) {
    e.stopPropagation()
    addtracersdetailsbtn.src = plusPath
    setCookie('add-tracers-details-shown', 'false')
  })
  addtracersdetails.addEventListener('show.bs.collapse', function (e) {
    e.stopPropagation()
    addtracersdetailsbtn.src = minusPath
    setCookie('add-tracers-details-shown', 'true')
  })
  shown = getCookie('add-tracers-details-shown')
  if (shown === 'true') { addtracersdetailsbtn.click() }

  const manualaddtracersdetails = document.getElementById('manual-add-tracers-details')
  const manualaddtracersdetailsbtn = document.getElementById('manual-add-tracers-details-button')
  manualaddtracersdetails.addEventListener('hide.bs.collapse', function (e) {
    e.stopPropagation()
    manualaddtracersdetailsbtn.src = plusPath
    setCookie('manual-add-tracers-details-shown', 'false')
  })
  manualaddtracersdetails.addEventListener('show.bs.collapse', function (e) {
    e.stopPropagation()
    manualaddtracersdetailsbtn.src = minusPath
    setCookie('manual-add-tracers-details-shown', 'true')
  })
  shown = getCookie('manual-add-tracers-details-shown')
  if (shown === 'true') { manualaddtracersdetailsbtn.click() }

  const infusatesdetails = document.getElementById('infusates-details')
  const infusatesdetailsbtn = document.getElementById('infusates-details-button')
  infusatesdetails.addEventListener('hide.bs.collapse', function (e) {
    e.stopPropagation()
    infusatesdetailsbtn.src = plusPath
    setCookie('infusates-details-shown', 'false')
  })
  infusatesdetails.addEventListener('show.bs.collapse', function (e) {
    e.stopPropagation()
    infusatesdetailsbtn.src = minusPath
    setCookie('infusates-details-shown', 'true')
  })
  shown = getCookie('infusates-details-shown')
  if (shown === 'true') { infusatesdetailsbtn.click() }

  const addinfusatesdetails = document.getElementById('add-infusates-details')
  const addinfusatesdetailsbtn = document.getElementById('add-infusates-details-button')
  addinfusatesdetails.addEventListener('hide.bs.collapse', function (e) {
    e.stopPropagation()
    addinfusatesdetailsbtn.src = plusPath
    setCookie('add-infusates-details-shown', 'false')
  })
  addinfusatesdetails.addEventListener('show.bs.collapse', function (e) {
    e.stopPropagation()
    addinfusatesdetailsbtn.src = minusPath
    setCookie('add-infusates-details-shown', 'true')
  })
  shown = getCookie('add-infusates-details-shown')
  if (shown === 'true') { addinfusatesdetailsbtn.click() }

  const manualaddinfusatesdetails = document.getElementById('manual-add-infusates-details')
  const manualaddinfusatesdetailsbtn = document.getElementById('manual-add-infusates-details-button')
  manualaddinfusatesdetails.addEventListener('hide.bs.collapse', function (e) {
    e.stopPropagation()
    manualaddinfusatesdetailsbtn.src = plusPath
    setCookie('manual-add-infusates-details-shown', 'false')
  })
  manualaddinfusatesdetails.addEventListener('show.bs.collapse', function (e) {
    e.stopPropagation()
    manualaddinfusatesdetailsbtn.src = minusPath
    setCookie('manual-add-infusates-details-shown', 'true')
  })
  shown = getCookie('manual-add-infusates-details-shown')
  if (shown === 'true') { manualaddinfusatesdetailsbtn.click() }

  const animalsdetails = document.getElementById('animals-details')
  const animalsdetailsbtn = document.getElementById('animals-details-button')
  animalsdetails.addEventListener('hide.bs.collapse', function (e) {
    e.stopPropagation()
    animalsdetailsbtn.src = plusPath
    setCookie('animals-details-shown', 'false')
  })
  animalsdetails.addEventListener('show.bs.collapse', function (e) {
    e.stopPropagation()
    animalsdetailsbtn.src = minusPath
    setCookie('animals-details-shown', 'true')
  })
  shown = getCookie('animals-details-shown')
  if (shown === 'true') { animalsdetailsbtn.click() }

  const samplesdetails = document.getElementById('samples-details')
  const samplesdetailsbtn = document.getElementById('samples-details-button')
  samplesdetails.addEventListener('hide.bs.collapse', function (e) {
    e.stopPropagation()
    samplesdetailsbtn.src = plusPath
    setCookie('samples-details-shown', 'false')
  })
  samplesdetails.addEventListener('show.bs.collapse', function (e) {
    e.stopPropagation()
    samplesdetailsbtn.src = minusPath
    setCookie('samples-details-shown', 'true')
  })
  shown = getCookie('samples-details-shown')
  if (shown === 'true') { samplesdetailsbtn.click() }

  const samplenamesdetails = document.getElementById('sample-names-details')
  const samplenamesdetailsbtn = document.getElementById('sample-names-details-button')
  samplenamesdetails.addEventListener('hide.bs.collapse', function (e) {
    e.stopPropagation()
    samplenamesdetailsbtn.src = plusPath
    setCookie('sample-names-details-shown', 'false')
  })
  samplenamesdetails.addEventListener('show.bs.collapse', function (e) {
    e.stopPropagation()
    samplenamesdetailsbtn.src = minusPath
    setCookie('sample-names-details-shown', 'true')
  })
  shown = getCookie('sample-names-details-shown')
  if (shown === 'true') { samplenamesdetailsbtn.click() }

  const sequencesdetails = document.getElementById('sequences-details')
  const sequencesdetailsbtn = document.getElementById('sequences-details-button')
  sequencesdetails.addEventListener('hide.bs.collapse', function (e) {
    e.stopPropagation()
    sequencesdetailsbtn.src = plusPath
    setCookie('sequences-details-shown', 'false')
  })
  sequencesdetails.addEventListener('show.bs.collapse', function (e) {
    e.stopPropagation()
    sequencesdetailsbtn.src = minusPath
    setCookie('sequences-details-shown', 'true')
  })
  shown = getCookie('sequences-details-shown')
  if (shown === 'true') { sequencesdetailsbtn.click() }

  const filesdetails = document.getElementById('files-details')
  const filesdetailsbtn = document.getElementById('files-details-button')
  filesdetails.addEventListener('hide.bs.collapse', function (e) {
    e.stopPropagation()
    filesdetailsbtn.src = plusPath
    setCookie('files-details-shown', 'false')
  })
  filesdetails.addEventListener('show.bs.collapse', function (e) {
    e.stopPropagation()
    filesdetailsbtn.src = minusPath
    setCookie('files-details-shown', 'true')
  })
  shown = getCookie('files-details-shown')
  if (shown === 'true') { filesdetailsbtn.click() }

  const injectionsdetails = document.getElementById('injections-details')
  const injectionsdetailsbtn = document.getElementById('injections-details-button')
  injectionsdetails.addEventListener('hide.bs.collapse', function (e) {
    e.stopPropagation()
    injectionsdetailsbtn.src = plusPath
    setCookie('injections-details-shown', 'false')
  })
  injectionsdetails.addEventListener('show.bs.collapse', function (e) {
    e.stopPropagation()
    injectionsdetailsbtn.src = minusPath
    setCookie('injections-details-shown', 'true')
  })
  shown = getCookie('injections-details-shown')
  if (shown === 'true') { injectionsdetailsbtn.click() }
}

// Get a Cookie
const getCookie = (name) => (
  document.cookie.match('(^|;)\\s*' + name + '\\s*=\\s*([^;]+)')?.pop() || ''
)

// Set a Cookie
function setCookie (name, val) {
  document.cookie = name + '=' + val + '; path=/'
}
