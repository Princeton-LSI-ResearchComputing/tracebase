const urlPrefixDict = JSON.parse(
  document.getElementById('url_prefix_dict').textContent
)

function animalNameFormatter (value, row) { // eslint-disable-line no-unused-vars
  const k = 'animal_detail_url_prefix'
  let urlPrefix = urlPrefixDict[k]
  return '<a href=' + urlPrefix + row.animal_id + '>' + value + '</a>'
}

function sampleNameFormatter (value, row) { // eslint-disable-line no-unused-vars
  const k = 'sample_detail_url_prefix'
  let urlPrefix = urlPrefixDict[k]
  return '<a href=' + urlPrefix + row.sample_id + '>' + value + '</a>'
}

function tissueNameFormatter (value, row) { // eslint-disable-line no-unused-vars
  const k = 'tissue_detail_url_prefix'
  let urlPrefix = urlPrefixDict[k]
  return '<a href=' + urlPrefix + row.tissue_id + '>' + value + '</a>'
}

function infusateNameFormatter (value, row) { // eslint-disable-line no-unused-vars
  const k = 'infusate_detail_url_prefix'
  let urlPrefix = urlPrefixDict[k]
  if (row.infusate_id === null) {
    url = None
  } else {
    url = '<a href=' + urlPrefix + row.infusate_id + '>' + value + '</a>'
  }
  return url
}

function treatmentFormatter (value, row) { // eslint-disable-line no-unused-vars
  const k = 'protocol_detail_url_prefix'
  let urlPrefix = urlPrefixDict[k]
  return '<a href=' + urlPrefix + row.treatment_id + '>' + value + '</a>'
}

function msrunFormatter (value, row) { // eslint-disable-line no-unused-vars
  const k = 'msrunsample_detail_url_prefix'
  let urlPrefix = urlPrefixDict[k]
  return '<a href=' + urlPrefix + row.msrunsample_id + '>' + 'MSRun Details' + '</a>'
}

// format url based on id_name_list, items are seperated by ||
function format_url_for_id_name_list (value, urlPrefix) { // eslint-disable-line no-unused-vars
  let outputWithLink = []

  for (let i = 0; i < value.length; i++) {
    // get id and name for each item
    let objId = value[i].split('||')[0]
    let objName = value[i].split('||')[1]
    url = urlPrefix + objId
    objWithLink = '<a href=' + url + '>' + objName + '</a>'
    outputWithLink.push(objWithLink)
    }
    return outputWithLink
}

function studyListFormatter (value, row) { // eslint-disable-line no-unused-vars
  let studyList = row.study_id_name_list
  let urlPrefix = urlPrefixDict['study_detail_url_prefix']
  return format_url_for_id_name_list(studyList, urlPrefix)
}

function compoundListFormatter (value, row) { // eslint-disable-line no-unused-vars
  let compoundList = row.compound_id_name_list
  let urlPrefix = urlPrefixDict['compound_detail_url_prefix']
  return format_url_for_id_name_list(compoundList, urlPrefix)
}

function tracerListFormatter (value, row) { // eslint-disable-line no-unused-vars
  let tracerList = row.tracer_id_name_list
  let urlPrefix = urlPrefixDict['compound_detail_url_prefix']
  return format_url_for_id_name_list(tracerList, urlPrefix)
}

function infusateListFormatter (value, row) { // eslint-disable-line no-unused-vars
  let infusateList = row.infusate_id_name_list
  let urlPrefix = urlPrefixDict['infusate_detail_url_prefix']
  return format_url_for_id_name_list(infusateList, urlPrefix)
}

function treatmentListFormatter (value, row) { // eslint-disable-line no-unused-vars
  let treatmentList = row.treatment_id_name_list
  let urlPrefix = urlPrefixDict['treamnet_detail_url_prefix']
  return format_url_for_id_name_list(treatmentList, urlPrefix)
}
