const url_prefix_dict = JSON.parse(
  document.getElementById('url_prefix_dict').textContent
);

function animalNameFormatter (value, row) {
  let k = "animal_detail_url_prefix"
  let url_prefix = url_prefix_dict[k]
  return "<a href=" + url_prefix + row.animal_id  + ">"+ value + "</a>"
}

function sampleNameFormatter (value, row) {
  let k = "sample_detail_url_prefix"
  let url_prefix = url_prefix_dict[k]
  return "<a href=" + url_prefix + row.sample_id  + ">"+ value + "</a>"
}

function tissueNameFormatter (value, row) {
  let k = "tissue_detail_url_prefix"
  let url_prefix = url_prefix_dict[k]
  return "<a href=" + url_prefix + row.tissue_id  + ">"+ value + "</a>"
}

function infusateNameFormatter (value, row) {
  let k = "infusate_detail_url_prefix"
  let url_prefix = url_prefix_dict[k]
  if (row.infusate_id === null) {
    url = None
  } else {
    url = "<a href=" + url_prefix + row.infusate_id  + ">"+ value + "</a>"
  }
  return url 
}

function treatmentFormatter (value, row) {
  let k = "protocol_detail_url_prefix"
  let url_prefix = url_prefix_dict[k]
  return "<a href=" + url_prefix + row.treatment_id  + ">"+ value + "</a>"
}

function msrunFormatter (value, row) {
  let k = "msrunsample_detail_url_prefix"
  let url_prefix = url_prefix_dict[k]
  return "<a href=" + url_prefix + row.msrunsample_id  + ">"+ "MSRun Details" + "</a>"
}

// format url based on id_name_list, items are seperated by ||
function format_url_for_id_name_list (value, url_prefix) {
  let output_with_link = []
  
  for (let i = 0; i < value.length; i++) {
    let obj_array = []
    // get id and name for each item
    obj_id = value[i].split('||')[0];
    obj_name = value[i].split('||')[1];
    url = url_prefix + obj_id;
    obj_with_link= "<a href=" + url + ">" + obj_name + "</a>";
    output_with_link.push(obj_with_link);
    }
    return output_with_link
}

function studyListFormatter (value, row) {
  study_id_name_list = row.study_id_name_list
  url_prefix = url_prefix_dict["study_detail_url_prefix"]
  return format_url_for_id_name_list(study_id_name_list, url_prefix)
}

function compoundListFormatter (value, row) {
  compound_id_name_list = row.compound_id_name_list
  url_prefix = url_prefix_dict["compound_detail_url_prefix"]
  return format_url_for_id_name_list(compound_id_name_list, url_prefix)
}

function tracerListFormatter (value, row) {
  tracer_id_name_list = row.tracer_id_name_list
  url_prefix = url_prefix_dict["compound_detail_url_prefix"]
  return format_url_for_id_name_list(tracer_id_name_list, url_prefix)
}

function infusateListFormatter (value, row) {
  infusate_id_name_list = row.infusate_id_name_list
  url_prefix = url_prefix_dict["infusate_detail_url_prefix"]
  return format_url_for_id_name_list(infusate_id_name_list, url_prefix)
}

function treatmentListFormatter (value, row) {
  treatement_id_name_list = row.treatment_id_name_list
  url_prefix = url_prefix_dict["treamnet_detail_url_prefix"]
  return format_url_for_id_name_list(treatment_id_name_list, url_prefix)
}
