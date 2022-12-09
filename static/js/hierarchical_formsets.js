/* Exported functions:
 *   saveSearchQueryHierarchy
 *   init
 * These methods must be called from javascript in a template after DOM content has loaded.
 */

// Globals

const minuspngpath = '/static/images/minus.png'
const pluspngpath = '/static/images/plus.png'
const pluspluspngpath = '/static/images/plusplus.png'
const infopngpath = '/static/images/status-question.png'
const infopngwidth = 10
const infopngheight = 10

// This is the default root of the form hierarchy.
// It should be initialized in the template.
// Provided here for structure reference.
// const rootGroup = {
//   selectedtemplate: 'pgtemplate',
//   searches: {
//     pgtemplate: {
//       name: 'PeakGroups',
//       tree: {
//         type: 'group',
//         val: 'all',
//         static: false,
//         queryGroup: []
//       }
//     },
//     ...
//   }
// }
// Linting is disabled for the disallowance of 'no-var' because let and const don't work here

var rootGroup = {} // eslint-disable-line no-var
var ncmpChoices = {} // eslint-disable-line no-var
var fldTypes = {} // eslint-disable-line no-var
var formErrLabel // eslint-disable-line no-var
var fmtSelectElem // eslint-disable-line no-var
var fldChoices = {} // eslint-disable-line no-var
var fldUnits = {} // eslint-disable-line no-var

/**
 * This initializes all of the global variables.
 */
function init (rootGroup, ncmpChoices, fldTypes, fldChoices, fldUnits) { // eslint-disable-line no-unused-vars
  globalThis.rootGroup = rootGroup
  globalThis.ncmpChoices = ncmpChoices
  globalThis.fldTypes = fldTypes
  globalThis.fldChoices = fldChoices
  globalThis.fldUnits = fldUnits
  globalThis.formErrLabel = document.getElementById('formerror')
}

/**
 * Given a format/template ID, this "shows" the search form for that format and hides all the others
 */
function showOutputFormatSearch (shownTemplateId) {
  const shownHierarchyId = shownTemplateId + '-hierarchy'
  for (const templateId of Object.keys(rootGroup.searches)) {
    const hierarchyId = templateId + '-hierarchy'
    const hierDiv = document.getElementById(hierarchyId)
    if (shownHierarchyId === hierarchyId) {
      hierDiv.style = ''
    } else {
      hierDiv.style = 'display:none;'
    }
  }
}

/**
 * This method adds a child form to the hierarchical form structure.  It either adds a single query or a group.  If adding a group, it adds either 1 or 2 queries inside the group (1 if it's the root group, 2 otherwise).  This is primarily used by the buttons via a listener.
 *   element [required] is an existing DOM object.
 *   templateId [required] indicates the hierarchy to which a search query is being added.
 *   query [required] is either an child object node that is being added to a data structure that tracks the hierarchy, or it is an existing sibling node after which a sibling is being added (depending on the value of 'afterMode').
 *   parentGroup [optional] is the parent object node of the hierarchy-tracking data structure used to determine where a sibling is to be inserted or a child node is to be appended (depending on the value of 'afterMode').  Root is assumed if not supplied.
 *   afterMode [optional] determines whether a sibling will be created & inserted after query (if true) or if query will be appended as a child to parentGroup (if false).  Default = false.
 */
function appendInnerSearchQuery (element, templateId, query, parentGroup, afterMode) {
  'use strict'

  if (typeof afterMode === 'undefined') {
    afterMode = false
  }

  let isRoot = true
  if (typeof parentGroup !== 'undefined' || parentGroup) {
    isRoot = false
  }

  let isGroup = false
  let myDiv

  if (('' + query.type) === 'group') {
    isGroup = true
    myDiv = createGroupSelectListDiv(templateId, query, parentGroup)
  } else if (('' + query.type) === 'query') {
    myDiv = createSearchFieldFormDiv(templateId, query, parentGroup)
  } else {
    formErrLabel.innerHTML = 'Error: Unrecognized query type: ' + query.type
  }

  if (isGroup) {
    // Add a couple queries to start off
    const subQuery = {
      type: 'query',
      val: ''
    }
    query.queryGroup.push(subQuery)
    appendInnerSearchQuery(myDiv, templateId, subQuery, query)

    // If this isn't the root, append a second query form
    if (!isRoot) {
      const subQuery2 = {
        type: 'query',
        val: ''
      }
      query.queryGroup.push(subQuery2)
      appendInnerSearchQuery(myDiv, templateId, subQuery2, query)
    }

    // Not exactly sure why, but after adding inner elements to a group, an empty div is needed to make future dynamically-added form elements to be correctly created.  I did this based on the template post I followed that had a static empty div just inside where the dynamic content was being created, when stuff I was adding wasn't working right and it seems to have fixed it.
    myDiv.append(document.createElement('div'))

    // Initialization using a copied rootgroup adds items one at a time, so don't add the follow-up + and ++ buttons.  This way, the individually eppended inner forms don't go under these buttons.  This means that the initializing function must add these manually.
    if (!isRoot) {
      addQueryAndGroupAddButtons(myDiv, query, parentGroup, templateId)
    }
  }

  if (afterMode) {
    element.after(myDiv)
  } else {
    element.appendChild(myDiv)
  }
}

/**
 * This creates a form for a database field search, including a fld select list, an ncmp select list, and a series of widgets for the "val" (search term).  It adds that form to the given div.
 */
function addSearchFieldForm (myDiv, query, templateId) {
  // Clone the form template
  const templateDiv = document.querySelector('#' + templateId)
  const elements = templateDiv.querySelectorAll('input,select,textarea')
  const clones = []
  elements.forEach(function (elem) {
    clones.push(elem.cloneNode(true))
  })

  // For each clones input form element
  var fldClone // eslint-disable-line no-var
  let fldInitVal = ''
  var ncmpClone // eslint-disable-line no-var
  let ncmpInitVal = ''
  var unitsClone // eslint-disable-line no-var
  let unitsInitVal = ''
  var valClone // eslint-disable-line no-var
  let isBlankForm = false
  for (let i = 0; i < clones.length; i++) {
    // If an invalid form was previously submitted, we will need to present errors
    const errors = []

    // Dismiss any previous error (that was previously presented and prevented)
    clones[i].addEventListener('click', function () {
      formErrLabel.innerHTML = ''
    })

    // Keep the value of the hierarchy structure up to date when the user changes the form value
    clones[i].addEventListener('change', function (event) {
      query[clones[i].name] = event.target.value
    })

    const keyname = clones[i].name.split('-').pop()

    // The field choices are static. Once they're set for any added field, they no longer need updating - only hidden
    // or revealed.  They must be updated before setting the default value.
    if (keyname === 'fld') {
      updateFldChoices(templateId, clones[i])
    }

    // Initialize the value in the hierarchy with the default
    if (typeof query[keyname] !== 'undefined' && query[keyname]) {
      clones[i].value = query[keyname]
    } else {
      query[clones[i].name] = clones[i].value
    }

    // Grab a reference to the values needed for inter-dependent dynamic update below
    if (keyname === 'fld') {
      fldInitVal = clones[i].value
      fldClone = clones[i]
    } else if (keyname === 'ncmp') {
      ncmpInitVal = clones[i].value
      ncmpClone = clones[i]
    } else if (keyname === 'units') {
      // For initial blank forms, clones[i].value for units is based on valClone.value, which has not been set yet in
      // the loop, but its initial value automatically defaults to the *first* item in the select list
      unitsInitVal = clones[i].value
      unitsClone = clones[i]
    } else if (keyname === 'val') {
      if (clones[i].value === '') {
        isBlankForm = true
      }
      valClone = clones[i]
      // Hide the val text field
      clones[i].style = 'display:none;'
    }

    // If the query is static (as defined in the Format class), disable its input element
    if (query.static) {
      clones[i].disabled = true
    }

    // Add this row to the HTML form
    myDiv.appendChild(clones[i])
    myDiv.appendChild(document.createTextNode(' '))

    // If there were any errors, create an error label
    // For some reason, this was a nice tooltip in an earlier version (f9c2cac151f9909380022cea8b7a40a5f0e72a4e), but
    // doesn't work automatically in the latest version
    if (errors.length > 0) {
      const errlabel = document.createElement('label')
      errlabel.className = 'text-danger'
      errlabel.innerHTML = ''
      for (let j = 0; j < errors.length; j++) {
        errlabel.innerHTML += errors[j] + ' '
      }
      myDiv.appendChild(errlabel)
    }
  }

  // If fldInitVal is an empty string (which is true for template(s) that are not currently selected in the fmt select
  // list), check and initialize arbitrarily to the first value in each select list
  if (fldInitVal === '') {
    fldInitVal = fldClone[0].value
    ncmpInitVal = ncmpClone[0].value
  }

  if (isBlankForm) {
    // fldUnits contains a default for each template/field combo
    unitsInitVal = fldUnits[templateId][fldInitVal].default
  }

  // Initialize the ncmp choices
  updateNcmpChoices(fldInitVal, ncmpClone, templateId)
  ncmpClone.value = ncmpInitVal

  // Initialize the units choices
  unitsClone.value = unitsInitVal
  const unitsInfoImg = document.createElement('img')
  unitsInfoImg.src = infopngpath
  unitsInfoImg.width = infopngwidth
  unitsInfoImg.height = infopngheight
  const infoSpan = document.createElement('span')
  infoSpan.style = 'display:none;'
  infoSpan.className = 'unitsinfo'
  infoSpan.appendChild(unitsInfoImg)
  infoSpan.appendChild(document.createTextNode(' '))
  const tooltipSpan = document.createElement('span')
  tooltipSpan.className = 'unitstooltip'
  infoSpan.appendChild(tooltipSpan)
  myDiv.appendChild(infoSpan)
  updateUnitsChoices(fldInitVal, ncmpInitVal, unitsInitVal, unitsClone, templateId)
  updateUnitsAboutInfo(unitsInitVal, fldInitVal, ncmpInitVal, templateId, infoSpan, tooltipSpan)
  attachInfoTooltip(unitsInfoImg, tooltipSpan)

  // Initialize the val field(s)
  const valFields = updateValFields(fldInitVal, ncmpInitVal, unitsClone, valClone, templateId)

  // Keep the ncmp select list choices updated to reflect the fld value
  fldClone.addEventListener('change', function (event) {
    updateNcmpChoices(event.target.value, ncmpClone, rootGroup.selectedtemplate)
    updateUnitsChoices(event.target.value, ncmpClone.value, '', unitsClone, rootGroup.selectedtemplate)
    attachInfoTooltip(unitsInfoImg, tooltipSpan)
    updateUnitsAboutInfo(unitsClone.value, fldClone.value, ncmpClone.value, rootGroup.selectedtemplate, infoSpan, tooltipSpan)
    updateValFields(event.target.value, ncmpClone.value, unitsClone, valClone, rootGroup.selectedtemplate, valFields)
  })

  // Keep the val fields updated to also reflect the ncmp value (currently only affected by values isnull and not_isnull)
  ncmpClone.addEventListener('change', function (event) {
    updateValFields(fldClone.value, event.target.value, unitsClone, valClone, rootGroup.selectedtemplate, valFields)
    updateUnitsChoices(fldClone.value, event.target.value, '', unitsClone, rootGroup.selectedtemplate)
    updateUnitsAboutInfo(unitsClone.value, fldClone.value, ncmpClone.value, rootGroup.selectedtemplate, infoSpan, tooltipSpan)
  })

  // Keep the val fields updated to also reflect the ncmp value (currently only affected by values isnull and not_isnull)
  unitsClone.addEventListener('change', function (event) {
    // This is to update the placeholder in the valFields
    updateValFields(fldClone.value, event.target.value, unitsClone, valClone, rootGroup.selectedtemplate, valFields)
    attachInfoTooltip(unitsInfoImg, tooltipSpan)
    updateUnitsAboutInfo(event.target.value, fldClone.value, ncmpClone.value, rootGroup.selectedtemplate, infoSpan, tooltipSpan)
  })
}

/**
 * This returns a div containing a form for a database field search, including a fld select list, an ncmp select list, and a series of widgets for the "val" (search term).  It also adds controls for creating or removing search forms and groups of forms whose terms are "and-ed" or "or-ed" together.
 */
function createSearchFieldFormDiv (templateId, query, parentGroup) {
  const myDiv = document.createElement('div')

  if (('' + query.type) !== 'query') {
    console.error("The supplied query must be of type 'query'.")
    return myDiv
  }

  myDiv.className = 'level-indent'

  addSearchFieldForm(myDiv, query, templateId)

  if (!query.static && !parentGroup.static) {
    addRemoveButton(myDiv, query, parentGroup)
  }

  if (!parentGroup.static) {
    addQueryAndGroupAddButtons(myDiv, query, parentGroup, templateId)
  }

  return myDiv
}

/**
 * This function coordinates the update of all val form widgets, showing and hiding based on fld type and ncmp selection
 */
function updateValFields (fldInitVal, ncmpInitVal, unitsClone, valClone, templateId, valFields) {
  const dbFieldType = getDBFieldType(templateId, fldInitVal)
  const dbFieldChoices = getDBEnumFieldChoices(templateId, fldInitVal)

  const unitsVal = unitsClone.value
  let placeholder = fldUnits[templateId][fldInitVal].metadata[unitsVal].example
  if (typeof placeholder === 'undefined' || !placeholder || placeholder === '') {
    if (dbFieldType === 'string') {
      placeholder = 'search term'
    } else {
      placeholder = 'search value'
    }
  } else {
    placeholder = 'e.g. "' + placeholder + '"'
  }

  let isAddMode = false
  // Create custom field for the val input, to be shown/hidden based on the other select-list selections
  if (typeof valFields === 'undefined' || !valFields) {
    isAddMode = true
    valFields = {}

    // For string and number fld types when ncmp is not (isnull or not_isnull)
    valFields.valTextBox = document.createElement('input')
    valFields.valTextBox.placeholder = placeholder
    valFields.valTextBox.value = valClone.value

    // For string, number, and enumeration fld types when ncmp is (isnull or not_isnull)
    valFields.valHiddenBox = document.createElement('input')
    valFields.valHiddenBox.style = 'display:none;'
    valFields.valHiddenBox.placeholder = placeholder
    valFields.valHiddenBox.value = 'dummy'
    // No listener needed for the hidden dummy field

    // For enumeration fld types when ncmp is not (isnull or not_isnull)
    valFields.valSelectList = document.createElement('select')
    updateValEnumSelectList(valFields.valSelectList, dbFieldChoices)

    valFields.valTextBox.addEventListener('change', function (event) {
      valClone.value = event.target.value
    })
    valFields.valSelectList.addEventListener('change', function (event) {
      valClone.value = event.target.value
    })
  }

  if (ncmpInitVal === 'isnull' || ncmpInitVal === 'not_isnull') {
    valClone.value = valFields.valHiddenBox.value
    valFields.valTextBox.style = 'display:none;'
    valFields.valSelectList.style = 'display:none;'
  } else if (dbFieldType === 'string' || dbFieldType === 'number') {
    // If the initval was 'dummy' empty it out (note, this will fail if the user actually wants to search for 'dummy')
    if (valFields.valTextBox.value === 'dummy') {
      valClone.value = ''
      valFields.valTextBox.value = ''
    } else {
      valClone.value = valFields.valTextBox.value
    }
    valFields.valTextBox.style = ''
    valFields.valSelectList.style = 'display:none;'
    if (dbFieldType === 'string') {
      valFields.valTextBox.placeholder = placeholder
    } else {
      valFields.valTextBox.placeholder = placeholder
    }
  } else if (dbFieldType === 'enumeration') {
    updateValEnumSelectList(valFields.valSelectList, dbFieldChoices, valClone)
    // The valClone.value may change if the field was changed from its initial value, because it is a different select
    // list, which defaults to the first option
    valClone.value = valFields.valSelectList.value
    valFields.valTextBox.style = 'display:none;'
    valFields.valSelectList.style = ''
  } else {
    console.error('Undetermined database field type for template/field-path:', templateId, fldInitVal, 'Unable to determine val form field type.')
    valFields.valTextBox.style = ''
    valFields.valSelectList.style = ''
  }

  if (isAddMode) {
    valClone.after(valFields.valSelectList)
    valClone.after(valFields.valHiddenBox)
    valClone.after(valFields.valTextBox)
  }

  if (valClone.disabled) {
    valFields.valTextBox.disabled = true
    valFields.valSelectList.disabled = true
  }

  return valFields
}

/**
 * This function (re)populates an enumeration type field's val select list based on the selected fld and ncmp values
 */
function updateValEnumSelectList (valSelectList, dbFieldChoices, valClone) {
  const arrOptions = []
  let valSupplied = false
  if (typeof valClone !== 'undefined' && valClone && typeof valClone.value !== 'undefined' && valClone.value) {
    valSupplied = true
  }
  let valExistsInChoices = false
  for (let i = 0; i < dbFieldChoices.length; i++) {
    arrOptions.push('<option value="' + dbFieldChoices[i][0] + '">' + dbFieldChoices[i][1] + '</option>')

    // Right now, this only handles initializing the select list where the values are strings or numbers, so in order
    // to get booleans to work, I check both the actual value: dbFieldChoices[i][0] and the item name:
    // dbFieldChoices[i][1] to match the names when they are "true" or "false"
    if (valSupplied && (dbFieldChoices[i][0] === valClone.value || dbFieldChoices[i][1] === valClone.value)) {
      valExistsInChoices = true
    }
  }
  let initVal = 'unable to set'
  // If there is an initial value from valClone (i.e. loading the executed search results and setting up the form)
  if (dbFieldChoices.length > 0) {
    if (valSupplied && valExistsInChoices) {
      initVal = valClone.value
    } else {
      initVal = dbFieldChoices[0][0]
    }
  }
  // See if the select list os the same as it was during the initial load (because it changes when the field select
  // list or comparison select list is changed)
  const theSame = isValEnumSelectListTheSame(valSelectList, dbFieldChoices)
  if (!theSame) {
    if (dbFieldChoices.length > 0) {
      valSelectList.innerHTML = arrOptions.join('')
      // Cannot use the valClone init val (if it was originally supplied because the select list has changed from its
      // initial value)
      valSelectList.value = initVal
    } else {
      // We don't have anything to populate the select list with, so clear out the options
      valSelectList.innerHTML = ''
      valSelectList.value = initVal
    }
  } else {
    // The select list is the same as it was during the initial load, so we can set the initial value
    valSelectList.value = initVal
  }
}

/**
 * The purpose of this is to re-use the enumeration's select list if the user has switched away from an enumeration
 * field and then back to the same enumeration field.  This preserves the previous selection.  One limitation of this
 * approach is that it doesn't preserve selections when switching between different enumeration fields (but string/
 * number values aren't preserved either, so NBD).
 */
function isValEnumSelectListTheSame (valSelectList, dbFieldChoices) {
  let theSame = true
  if (valSelectList.length === 0 || valSelectList.length !== dbFieldChoices.length) {
    theSame = false
  } else {
    const opts = valSelectList.options
    for (let i = 0; i < dbFieldChoices.length; i++) {
      if (opts[i].value !== dbFieldChoices[i][0] || opts[i].innerHTML !== dbFieldChoices[i][1]) {
        theSame = false
        break
      }
    }
  }
  return theSame
}

/**
 * This function uses the fldTypes global to return the type of a DB field's form type (string, number, or enumeration) that is used to show/hide the correct val form widget.
 */
function getDBFieldType (templateId, fldVal) {
  return fldTypes[templateId][fldVal].type
}

/**
 * For fields of type "enumeration", this function uses the fldTypes global to return the 2D choices array that can be used to populate a "val" select list
 */
function getDBEnumFieldChoices (templateId, fldVal) {
  return fldTypes[templateId][fldVal].choices
}

/**
 * Uses the fldTypes global variable to (re)populate the supplied ncmp select list
 */
function updateNcmpChoices (fldVal, ncmpSelectElem, templateId) {
  let fldtype = ''
  let choices = []
  if (typeof fldTypes[templateId] !== 'undefined' && fldTypes[templateId]) {
    if (typeof fldTypes[templateId][fldVal] !== 'undefined' && fldTypes[templateId][fldVal]) {
      if (typeof fldTypes[templateId][fldVal].type !== 'undefined' && fldTypes[templateId][fldVal].type) {
        fldtype = fldTypes[templateId][fldVal].type
        if (typeof ncmpChoices[fldtype] !== 'undefined' && ncmpChoices[fldtype]) {
          choices = ncmpChoices[fldtype]
        } else {
          console.error('Type', fldtype, 'for field', fldVal, 'in field type lookup for template', templateId, 'does not have select list values defined in ncmpChoices.')
        }
      } else {
        console.error('Type not defined for selected field', fldVal, 'in field type lookup for template', templateId)
      }
    } else {
      console.error('Selected field', fldVal, 'not in field type lookup for template', templateId)
    }
  } else {
    console.error('Template', templateId, 'not in field type lookup.')
  }

  populateSelectList(choices, ncmpSelectElem)
}

/**
 * Uses the fldTypes global variable to (re)populate the supplied ncmp select list
 */
function updateUnitsChoices (fldVal, ncmpVal, unitsVal, unitsSelectElem, templateId) {
  let choices = []
  const origUnitsVal = unitsSelectElem.value
  const defUnitsVal = fldUnits[templateId][fldVal].default

  if (typeof fldUnits[templateId] !== 'undefined' && fldUnits[templateId]) {
    if (typeof fldUnits[templateId][fldVal] !== 'undefined' && fldUnits[templateId][fldVal]) {
      choices = fldUnits[templateId][fldVal].choices

      // Hide if there is only 1 option in the select list
      if (choices.length < 2) {
        unitsSelectElem.style = 'display:none;'
      } else if (ncmpVal === 'isnull' || ncmpVal === 'not_isnull') {
        unitsSelectElem.style = 'display:none;'
      } else {
        // Show the units select list
        unitsSelectElem.style = ''
      }
    } else {
      console.error('Selected field', fldVal, 'not in field units lookup for template', templateId)
    }
  } else {
    console.error('Template', templateId, 'not in field type lookup.')
  }

  const vals = populateSelectList(choices, unitsSelectElem)

  if (typeof unitsVal !== 'undefined' && unitsVal && unitsVal !== '' && vals.includes(unitsVal)) {
    // If an explicit value was provided and it exists among the potentially changed options
    unitsSelectElem.value = unitsVal
  } else if (typeof origUnitsVal !== 'undefined' && origUnitsVal && vals.includes(origUnitsVal)) {
    // If the select list had value and it still exists among the potentially changed options
    unitsSelectElem.value = origUnitsVal
  } else if (typeof defUnitsVal !== 'undefined' && defUnitsVal && vals.includes(defUnitsVal)) {
    // If the default value is defined and it still exists among the potentially changed options
    unitsSelectElem.value = defUnitsVal
  } else if (unitsSelectElem.value === 'undefined' || !unitsSelectElem.value) {
    // Fall back to the first item in the choices and issue an error
    unitsSelectElem.value = vals[0]
    console.error('Invalid or no value for units select list.  Defaulting to first item.')
  }
}

/**
 * Uses the fldTypes global variable to (re)populate the supplied ncmp select list
 */
function updateUnitsAboutInfo (unitsVal, fldVal, ncmpVal, templateId, infoSpan, tooltipSpan) {
  let choices = []
  if (typeof fldUnits[templateId] !== 'undefined' && fldUnits[templateId]) {
    if (typeof fldUnits[templateId][fldVal] !== 'undefined' && fldUnits[templateId][fldVal]) {
      choices = fldUnits[templateId][fldVal].choices

      let tooltipContent = ''
      if (typeof fldUnits[templateId][fldVal].metadata[unitsVal] !== 'undefined' && fldUnits[templateId][fldVal].metadata[unitsVal]) {
        tooltipContent = fldUnits[templateId][fldVal].metadata[unitsVal].about
        tooltipSpan.innerHTML = tooltipContent
      } else {
        console.warn('The supplied value for units:', unitsVal, 'is not a valid option.  Available units options for field ', fldVal, 'are:', Object.keys(fldUnits[templateId][fldVal].metadata), 'Setting about info to empty.')
        tooltipSpan.innerHTML = ''
      }

      // Hide if there is only 1 option in the select list
      if (choices.length < 2) {
        infoSpan.style.display = 'none'
      } else if (ncmpVal === 'isnull' || ncmpVal === 'not_isnull') {
        infoSpan.style.display = 'none'
      } else {
        // See if there is about info
        if (typeof tooltipContent === 'undefined' || !tooltipContent || tooltipContent === 'null') {
          infoSpan.style.display = 'none'
        } else {
          infoSpan.style.display = ''
        }
      }
    } else {
      console.error('Selected field', fldVal, 'not in field units lookup for template', templateId)
    }
  } else {
    console.error('Template', templateId, 'not in field type lookup.')
  }
}

/**
 * The fld choices for every database field select list needs to be pared down from all fields for every format to just
 * the format the database search term field is being added to.  It only needs to happen once - whenever a field select
 * list is created.
 */
function updateFldChoices (templateId, fldSelectElem) {
  let choices = []
  if (typeof fldChoices[templateId] !== 'undefined' && fldChoices[templateId]) {
    choices = fldChoices[templateId]
  } else {
    console.error('Template', templateId, 'not in field choices lookup.')
  }

  populateSelectList(choices, fldSelectElem)
}

/**
 * Populates a given select list with the given 2D array of choices
 */
function populateSelectList (choices, selectElem) {
  const vals = []
  if (choices.length > 0) {
    const arrOptions = []
    for (let i = 0; i < choices.length; i++) {
      arrOptions.push('<option value="' + choices[i][0] + '">' + choices[i][1] + '</option>')
      vals.push(choices[i][0])
    }
    selectElem.innerHTML = arrOptions.join('')
    selectElem.value = choices[0][0]
  }
  return vals
}

/**
 * Changes the browse link to refer to the supplied format
 */
function updateBrowseLink (templateId) {
  const blink = document.getElementById('browselink')
  // There is no browse link when in browse mode
  if (typeof blink !== 'undefined' && blink) {
    const regex = /format=[^;&]+/i
    let newhref = blink.href.replace(regex, 'format=' + templateId)
    if (newhref === blink.href) {
      newhref += '&format=' + templateId
    }
    blink.href = newhref
    blink.innerHTML = 'Browse All ' + rootGroup.searches[templateId].name
  }
}

/**
 * Adds the format select list to the supplied div
 */
function addFormatSelectList (parentDiv, rootQueryObject) {
  // Create the div that will contain the format select list
  const myDiv = document.createElement('div')
  myDiv.id = 'formatSelectionDiv'

  // If query is not supplied, default to the root group
  if (typeof rootQueryObject === 'undefined' || !rootQueryObject) {
    rootQueryObject = rootGroup
  }

  // Keep the browse link up to date with the selected format
  updateBrowseLink(rootQueryObject.selectedtemplate)

  // Create a format select list
  fmtSelectElem = document.createElement('select')
  fmtSelectElem.name = 'fmt'
  fmtSelectElem.id = 'formatSelection'
  for (const key of Object.keys(rootQueryObject.searches)) {
    const option = document.createElement('option')
    option.value = key
    option.text = rootQueryObject.searches[key].name
    fmtSelectElem.appendChild(option)
  }
  fmtSelectElem.value = rootQueryObject.selectedtemplate

  // Use a change as an opportunity to dismiss previous errors
  // And keep the selected value up to date in the object
  fmtSelectElem.addEventListener('change', function (event) {
    formErrLabel.innerHTML = ''
    rootQueryObject.selectedtemplate = event.target.value
    showOutputFormatSearch(rootQueryObject.selectedtemplate)
    updateBrowseLink(rootQueryObject.selectedtemplate)
  })

  // Put descriptive text in front of the select list
  const label1 = document.createElement('label')
  label1.innerHTML = 'Output Format: '

  // Add the group select list to the DOM
  myDiv.appendChild(label1)
  myDiv.appendChild(document.createTextNode(' '))
  myDiv.appendChild(fmtSelectElem)

  parentDiv.appendChild(myDiv)
}

/**
 * Adds an any/all select list to the supplied div.  It takes the relevant group from the rootGroup structure in order to keep it up to date.
 */
function addGroupSelectList (myDiv, group) {
  // Create a group type select list
  const grouptypes = ['all', 'any']
  const select = document.createElement('select')
  select.name = 'grouptype'
  for (const val of grouptypes) {
    const option = document.createElement('option')
    option.value = val
    option.text = val
    select.appendChild(option)
  }
  select.value = group.val
  if (group.static) {
    select.disabled = true
  }

  // Use a change as an opportunity to dismiss previous errors
  select.addEventListener('change', function (event) {
    formErrLabel.innerHTML = ''
    group.val = event.target.value
  })

  // Put descriptive text in front of the select list
  const label1 = document.createElement('label')
  label1.innerHTML = 'Match '
  label1.htmlFor = 'grouptype'

  // Add the group select list to the DOM
  myDiv.appendChild(label1)
  myDiv.appendChild(document.createTextNode(' '))
  myDiv.appendChild(select)
}

/**
 * Returns a div containing a group with an any/all select list.
 */
function createGroupSelectListDiv (templateId, group, parentGroup) {
  const myDiv = document.createElement('div')

  if (('' + group.type) !== 'group') {
    console.error("The supplied group must be of type 'group'.")
    return myDiv
  }

  let isRoot = true
  let isHidden = false
  if (typeof parentGroup !== 'undefined' || parentGroup) {
    isRoot = false
  } else {
    if (rootGroup.selectedtemplate !== templateId) {
      isHidden = true
    }
  }

  if (isRoot) {
    const templatename = templateId + '-hierarchy'
    myDiv.id = templatename
    if (isHidden) {
      myDiv.style = 'display:none;'
    }
  } else {
    myDiv.className = 'level-indent'
  }

  addGroupSelectList(myDiv, group)

  if (!isRoot && !group.static && !parentGroup.static) {
    addRemoveButton(myDiv, group, parentGroup)
  }

  return myDiv
}

/**
 * Adds a remove (-) button to the supplied div. It takes the query object and its parent group in order to keep the rootGroup structure in synch when those buttons are clicked.
 */
function addRemoveButton (myDiv, query, parentGroup) {
  const rmBtn = document.createElement('a')
  rmBtn.href = 'javascript:void(0)'
  const btnImg = document.createElement('img')
  btnImg.src = minuspngpath
  rmBtn.appendChild(btnImg)
  rmBtn.addEventListener('click', function (event) {
    formErrLabel.innerHTML = ''

    let size = 0
    for (let i = 0; i < parentGroup.queryGroup.length; i++) {
      if (!parentGroup.queryGroup[i].static) {
        size++
      }
    }
    if (size <= 1) {
      formErrLabel.innerHTML = 'A match group must have at least 1 query.'
    } else {
      event.target.parentNode.parentNode.remove()
      const index = parentGroup.queryGroup.indexOf(query)
      parentGroup.queryGroup.splice(index, 1)
    }
  })
  myDiv.appendChild(document.createTextNode(' '))
  myDiv.appendChild(rmBtn)
}

/**
 * Adds query (+) and group (++) buttons to the supplied div. It takes the query object and its parent group in order to keep the rootGroup structure in synch when those buttons are clicked.
 */
function addQueryAndGroupAddButtons (myDiv, query, parentGroup, templateId) {
  // Add query to a group (button)
  const termbtn = document.createElement('a')
  termbtn.href = 'javascript:void(0)'
  const pBtnImg = document.createElement('img')
  pBtnImg.src = pluspngpath
  termbtn.appendChild(pBtnImg)
  termbtn.addEventListener('click', function (event) {
    formErrLabel.innerHTML = ''

    const sibQuery = {
      type: 'query',
      val: ''
    }
    const index = parentGroup.queryGroup.indexOf(query)
    parentGroup.queryGroup.splice(index + 1, 0, sibQuery)
    // The clicked item is the image, so to get the eclosing div, we need the grandparent
    appendInnerSearchQuery(event.target.parentNode.parentNode, templateId, sibQuery, parentGroup, true)
  })
  myDiv.appendChild(document.createTextNode(' '))
  myDiv.appendChild(termbtn)

  // Add group to a group (button)
  const grpbtn = document.createElement('a')
  grpbtn.href = 'javascript:void(0)'
  const ppBtnImg = document.createElement('img')
  ppBtnImg.src = pluspluspngpath
  grpbtn.appendChild(ppBtnImg)
  grpbtn.addEventListener('click', function (event) {
    formErrLabel.innerHTML = ''

    const sibGroup = {
      type: 'group',
      val: 'any',
      queryGroup: []
    }
    const index = parentGroup.queryGroup.indexOf(query)
    parentGroup.queryGroup.splice(index + 1, 0, sibGroup)
    // The clicked item is the image, so to get the eclosing div, we need the grandparent
    appendInnerSearchQuery(event.target.parentNode.parentNode, templateId, sibGroup, parentGroup, true)
  })
  myDiv.appendChild(document.createTextNode(' '))
  myDiv.appendChild(grpbtn)
}

/**
 * This method is for building the hierarchical forms on the results page from the global rootGroup
 *   element is the DOM object to which the forms will be added
 */
function initializeRootSearchQuery (element) { // eslint-disable-line no-unused-vars
  'use strict'

  addFormatSelectList(element)

  for (const templateId of Object.keys(rootGroup.searches)) {
    initializeRootSearchQueryHelper(element, templateId, rootGroup.searches[templateId].tree)
  }
}

/**
 * This is a recursive method called by initializeRootSearchQuery.  It traverses the global rootGroup data structure to build the search forms.
 *   element is the DOM object to which the forms will be added
 *   templateId indicates the hierarchy to which a search query is being added.
 *   parentNode is a reference to the parent of the current position in the rootGroup object.
 *   queryGroup is the parentNode's list of children of the hierarchical form data structure.
 */
function initializeRootSearchQueryHelper (element, templateId, queryNode, parentNode) {
  'use strict'

  if (queryNode.type === 'group') {
    let isRoot = true
    if (typeof parentNode !== 'undefined' || parentNode) {
      isRoot = false
    }

    // Create the group select list
    const groupDiv = createGroupSelectListDiv(templateId, queryNode, parentNode)

    // Add this group's children recursively
    for (let i = 0; i < queryNode.queryGroup.length; i++) {
      initializeRootSearchQueryHelper(groupDiv, templateId, queryNode.queryGroup[i], queryNode)
    }

    // Initialization using a copied rootgroup adds items one at a time, so don't add the follow-up + and ++ buttons.  This way, the individually eppended inner forms don't go under these buttons.  This means that the initializing function must add these manually.
    if (!isRoot && !parentNode.static) {
      addQueryAndGroupAddButtons(groupDiv, queryNode, parentNode, templateId)
    }

    element.appendChild(groupDiv)

    // Not exactly sure why, but after adding inner elements to a group, an empty div is needed so that future dynamically-added form elements are correctly created.  I did this based on the template post I followed that had a static empty div just inside where the dynamic content was being created, when stuff I was adding wasn't working right and it seems to have fixed it.
    groupDiv.append(document.createElement('div'))
  } else if (queryNode.type === 'query') {
    // Create the search field form
    const queryDiv = createSearchFieldFormDiv(templateId, queryNode, parentNode)
    element.appendChild(queryDiv)
  } else {
    console.error('Unknown node type:', queryNode.type)
  }
}

/**
 * This function obtains the selected format from the fmt select list
 */
function getSelectedFormat () {
  const fmtSelect = document.getElementById('formatSelection')
  const selectedformat = '' + fmtSelect.value
  let valid = false
  for (const templateId of Object.keys(rootGroup.searches)) {
    if (selectedformat === templateId) {
      valid = true
    }
  }
  if (!valid) {
    console.error('Invalid selected format:', selectedformat)
    return 'none'
  }
  return selectedformat
}

/**
 * Given the format ID, this retrieves and returns the format's name, as recorded in the global rootGroup object.
 */
function getFormatName (fmt) {
  const formatName = rootGroup.searches[fmt].name
  if (formatName.includes('-') || formatName.includes('.')) {
    console.error('Format name', formatName, 'is not allowed to contain dots or dashes.')
  }
  return formatName
}

/**
 * saveSearchQueryHierarchy has 2 purposes:
 *   1. It renames DOM object IDs of the input form elements to indicate a serial form number in the format Django expects.  It also updates 1 meta form element that indicates the total number of forms.
 *   2. It saves each leaf's hierarchical path in a hidden input element named "pos".  The path is in the form of index.index.index... where <index> is the child index.  The single value (all or any) of inner nodes is saved in the pathin the form index-all.index-any.index, e.g. "0-all-0-any.0".
 * This method takes the outer DOM object that contains all the forms
 */
function saveSearchQueryHierarchy (divElem) { // eslint-disable-line no-unused-vars
  'use strict'

  const childDivs = divElem.querySelectorAll(':scope > div') // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

  const selectedformat = getSelectedFormat()

  let total = 0

  // This will traverse a hierarchy for each possible output format
  for (let i = 1; i < childDivs.length; i++) {
    total = saveSearchQueryHierarchyHelper(childDivs[i], '', total, 0, selectedformat)
  }

  // Only 1 form needs to have the total set, but depending on how the form was initialized, it could be any of these, so attempt to set them all
  const prefixes = ['form']
  for (const prefix of Object.keys(rootGroup.searches)) {
    prefixes.push(prefix)
  }
  for (const prefix of prefixes) {
    const formInput = document.getElementById('id_' + prefix + '-TOTAL_FORMS')
    if (typeof formInput !== 'undefined' && formInput) {
      formInput.value = total
    }
  }
}

/**
 * saveSearchQueryHierarchyHelper is a recursive helper method to saveSearchQueryHierarchy.  It takes:
 *   divElem - The DOM object that contains forms.
 *   path - a running path string to be stored in a leaf form's hidden 'pos' field.
 *   count - The serial form number used to set the form element ID to what Django expects.
 *   idx - The hierarchical node index, relative to the parent's child node array.
 *   selectedformat - The selected item in the fmt select list.
 *   curfmt - Should initially be undefined/unsupplied.  The format currently being saved.
 */
function saveSearchQueryHierarchyHelper (divElem, path, count, idx, selectedformat, curfmt) {
  'use strict'

  // If the div has a "-hierarchy" ID, we're at the root, so we can update the format name
  if (typeof divElem.id !== 'undefined' && divElem.id && divElem.id.includes('-hierarchy')) {
    curfmt = '' + divElem.id.split('-').shift()
  }

  const childDivs = divElem.querySelectorAll(':scope > div') // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

  // This gets inputs belonging to the parent
  const childInputs = divElem.childNodes

  let isForm = false
  let isAll = true
  let staticForm = false
  let staticGroup = false
  let posElem
  let stcElem
  for (let i = 0; i < childInputs.length; i++) {
    if (typeof childInputs[i].name !== 'undefined' && childInputs[i].name) {
      if (childInputs[i].name.includes('-pos')) {
        isForm = true
        count++
        posElem = childInputs[i]
      } else if (childInputs[i].name.includes('grouptype')) {
        if (childInputs[i].value === 'any') {
          isAll = false
        }
        if (childInputs[i].disabled) {
          // Form elements that are "static" (see the Format class) are explicitly set as disabled
          // Infer static form by presence of disabled attribute
          staticGroup = true
        }
      } else if (childInputs[i].name.includes('-static')) {
        stcElem = childInputs[i]
      } else if (childInputs[i].disabled) {
        // Form elements that are "static" (see the Format class) are explicitly set as disabled
        // Infer static search form if any other form element is disabled
        staticForm = true
      }
      if (childInputs[i].disabled) {
        // Form elements that are "static" (see the Format class) are explicitly set as disabled
        // Remove the disabled attribute so that the data submits
        childInputs[i].removeAttribute('disabled')
      }
      if (curfmt === selectedformat && childInputs[i].name.includes('-val') && childInputs[i].value === '') {
        formErrLabel.innerHTML = 'All fields are required'
      }
    }
  }

  if (path === '') {
    let fmt = curfmt
    const formatName = getFormatName(curfmt)
    // Set up the root of the path to indicate the output format
    if (selectedformat === curfmt) {
      fmt += '-' + formatName + '-selected'
    } else {
      fmt += '-' + formatName
    }
    path += fmt + '.' + idx
  } else {
    path += '.' + idx
  }

  // If this is a form from a Django formset (otherwise it's a hierarchy control level)
  if (isForm) {
    posElem.value = path

    // Infer the static value based on whether any search field form field (not pos or grouptype) was disabled
    if (staticForm) {
      stcElem.value = 'true'
    } else {
      stcElem.value = 'false'
    }

    for (let i = 0; i < childInputs.length; i++) {
      if (typeof childInputs[i].name !== 'undefined' && childInputs[i].name) {
        // Replace (e.g. "form-0-val" or "form-__prefix__-val") with "form-<count>-val"
        const re = /-0-|-__prefix__-/
        const replacement = '-' + (count - 1) + '-'
        if (childInputs[i].for) {
          childInputs[i].for = childInputs[i].for.replace(re, replacement)
        }
        if (childInputs[i].id) {
          const tmp = childInputs[i].id
          const newid = tmp.replace(re, replacement)
          childInputs[i].id = newid
        }
        if (childInputs[i].name) {
          childInputs[i].name = childInputs[i].name.replace(re, replacement)
        }
      }
    }
  } else {
    if (isAll) {
      path += '-all'
    } else {
      path += '-any'
    }
    if (staticGroup) {
      path += '-true'
    } else {
      path += '-false'
    }
  }

  // Recurse
  // Always traverse 1 less, because there's always an empty trailing div tag
  for (let i = 0; i < childDivs.length; i++) {
    if (childDivs[i].innerHTML !== '') {
      count = saveSearchQueryHierarchyHelper(childDivs[i], path, count, i, selectedformat, curfmt)
    }
  }

  return count
}

/**
 * Creates a new search group and adds it to the search hierarchy at the root level, and adds the supplied query as its
 * first member.  The original search groups is made to be its second member.  For example, if the search is:
 *    Match `any`:
 *        `animal` `is` `971`
 *        `time_collected` `>` `00:00:01`
 * ...and the arguments are:
 *    group = {type: 'group', pos: '', static: false, val: 'all'}
 *    query = {type: 'query', pos: '', static: false, ncmp: 'iexact', fld: 'is_last', val: 'false'}
 *    format = 'fctemplate'
 * ...the resulting search will be:
 *    Match `all`:
 *        `is_last` `is` `false`
 *        Match `any`:
 *            `animal` `is` `971`
 *            `time_collected` `>` `00:00:01`
 */
function reRootSearch (group, query, format) { // eslint-disable-line no-unused-vars
  const divElem = document.querySelector('.hierarchical-search')
  const childDivs = divElem.querySelectorAll(':scope > div') // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div
  const selectedformat = getSelectedFormat()

  let curfmt
  if (typeof divElem.id !== 'undefined' && divElem.id && divElem.id.includes('-hierarchy')) {
    curfmt = '' + divElem.id.split('-').shift()
  }

  // Start to build the new search forms
  // Create a new query div
  const queryDiv = createSearchFieldFormDiv(format, query, group)

  // Create a new root group (will not be indented)
  const groupDiv = createGroupSelectListDiv(format, group)

  // Add the query div to the new group
  groupDiv.appendChild(queryDiv)

  // Now remove the original (selected) root and append it to the new root group as a child
  for (let i = 1; i < childDivs.length; i++) {
    // Get current format
    if (typeof childDivs[i].id !== 'undefined' && childDivs[i].id && childDivs[i].id.includes('-hierarchy')) {
      curfmt = '' + childDivs[i].id.split('-').shift()
    }
    if (curfmt === selectedformat) {
      childDivs[i].remove()

      // Indent the pre-existing search
      childDivs[i].className = 'level-indent'
      childDivs[i].id = ''
      groupDiv.append(childDivs[i])
    }
  }

  // Append the new group to this hierarchy
  divElem.append(groupDiv)
}

/**
 * Removes all form elements from the search hierarchy from the supplied "selected format" if its val field is empty
 */
function removeIncompleteSearchForms () { // eslint-disable-line no-unused-vars
  'use strict'

  const divElem = document.querySelector('.hierarchical-search')

  const childDivs = divElem.querySelectorAll(':scope > div') // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

  const selectedformat = getSelectedFormat()

  // This will traverse a hierarchy for each possible output format
  for (let i = 1; i < childDivs.length; i++) {
    removeIncompleteSearchFormsHelper(childDivs[i], selectedformat)
  }
}

/**
 * Recursively removes all form elements from the search hierarchy from the supplied "selected format" if its val field is empty
 */
function removeIncompleteSearchFormsHelper (divElem, selectedformat, curfmt) {
  'use strict'

  // If the div has a "-hierarchy" ID, we're at the root, so we can update the format name
  if (typeof divElem.id !== 'undefined' && divElem.id && divElem.id.includes('-hierarchy')) {
    curfmt = '' + divElem.id.split('-').shift()
  }

  const childDivs = divElem.querySelectorAll(':scope > div') // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

  // Always traverse 1 less, because there's always an empty trailing div tag
  const numChildren = (childDivs.length - 1)

  // This gets inputs belonging to the parent
  const childInputs = divElem.childNodes
  let numRemoved = 0

  for (let i = 0; i < childInputs.length; i++) {
    if (typeof childInputs[i].name !== 'undefined' && childInputs[i].name) {
      if (curfmt === selectedformat && childInputs[i].name.includes('-val') && (typeof childInputs[i].value === 'undefined' || childInputs[i].value === '')) {
        removeSearchForm(divElem)
        numRemoved += 1
      }
    }
  }

  // Recurse
  // Always traverse 1 less, because there's always an empty trailing div tag
  for (let i = 0; i < numChildren; i++) {
    removeIncompleteSearchFormsHelper(childDivs[i], selectedformat, curfmt)
    // We will use numRemoved=0 to infer this is a group div and if its children are empty, remove it
    if (curfmt === selectedformat && numRemoved === 0 && isSearchEmptyHelper(childDivs[i], selectedformat, curfmt)) {
      divElem.remove()
    }
  }
}

/**
 * Removes a single search form by removing the surrounding div tag
 */
function removeSearchForm (myDiv) {
  myDiv.remove()
}

/**
 * Determines if all search forms' val fields for the selected format are empty and returns true or false
 */
function isSearchEmpty () { // eslint-disable-line no-unused-vars
  'use strict'

  const divElem = document.querySelector('.hierarchical-search')

  const childDivs = divElem.querySelectorAll(':scope > div') // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

  const selectedformat = getSelectedFormat()

  let empty = true

  // This will traverse a hierarchy for each possible output format
  for (let i = 1; i < childDivs.length; i++) {
    empty = isSearchEmptyHelper(childDivs[i], selectedformat)
    if (!empty) {
      break
    }
  }

  return empty
}

/**
 * Recursively determines if all search forms' val fields in the supplied selected format are empty and returns true or false
 */
function isSearchEmptyHelper (divElem, selectedformat, curfmt) {
  'use strict'

  let empty = true

  // If the div has a "-hierarchy" ID, we're at the root, so we can update the format name
  if (typeof divElem.id !== 'undefined' && divElem.id && divElem.id.includes('-hierarchy')) {
    curfmt = '' + divElem.id.split('-').shift()
  }

  const childDivs = divElem.querySelectorAll(':scope > div') // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

  // Always traverse 1 less, because there's always an empty trailing div tag
  const numChildren = (childDivs.length - 1)

  // This gets inputs belonging to the parent
  const childInputs = divElem.childNodes

  for (let i = 0; i < childInputs.length; i++) {
    if (typeof childInputs[i].name !== 'undefined' && childInputs[i].name) {
      if (curfmt === selectedformat && childInputs[i].name.includes('-val') && childInputs[i].value !== '') {
        return false
      }
    }
  }

  // Recurse
  // Always traverse 1 less, because there's always an empty trailing div tag
  for (let i = 0; i < numChildren; i++) {
    empty = isSearchEmptyHelper(childDivs[i], selectedformat, curfmt)
    if (!empty) {
      return empty
    }
  }

  return empty
}

/**
 * Removes all form elements from the search hierarchy matching the supplied field from the selected format
 */
function removeFieldSearchForms (field) { // eslint-disable-line no-unused-vars
  'use strict'

  const divElem = document.querySelector('.hierarchical-search')

  const childDivs = divElem.querySelectorAll(':scope > div') // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

  const selectedformat = getSelectedFormat()

  // This will traverse a hierarchy for each possible output format
  for (let i = 1; i < childDivs.length; i++) {
    removeFieldSearchFormsHelper(childDivs[i], field, selectedformat)
  }
}

/**
 * Recursively removes all form elements from the search hierarchy matching the supplied field from the supplied "selected format"
 */
function removeFieldSearchFormsHelper (divElem, field, selectedformat, curfmt) {
  'use strict'

  // If the div has a "-hierarchy" ID, we're at the root, so we can update the format name
  if (typeof divElem.id !== 'undefined' && divElem.id && divElem.id.includes('-hierarchy')) {
    curfmt = '' + divElem.id.split('-').shift()
  }

  const childDivs = divElem.querySelectorAll(':scope > div') // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

  // Always traverse 1 less, because there's always an empty trailing div tag
  const numChildren = (childDivs.length - 1)

  // This gets inputs belonging to the parent
  const childInputs = divElem.childNodes
  let numRemoved = 0

  for (let i = 0; i < childInputs.length; i++) {
    if (typeof childInputs[i].name !== 'undefined' && childInputs[i].name) {
      if (curfmt === selectedformat && childInputs[i].name.includes('-fld') && childInputs[i].value === field) {
        removeSearchForm(divElem)
        numRemoved += 1
      }
    }
  }

  // Recurse
  // Always traverse 1 less, because there's always an empty trailing div tag
  for (let i = 0; i < numChildren; i++) {
    removeFieldSearchFormsHelper(childDivs[i], field, selectedformat, curfmt)
    // We will use numRemoved=0 to infer this is a group div and if its children are empty, remove it
    if (curfmt === selectedformat && numRemoved === 0 && isSearchEmptyHelper(childDivs[i], selectedformat, curfmt)) {
      divElem.remove()
    }
  }
}

/**
 * This marches through all search hierarchies to find the first occurrence on the given field in the selected format's search hierarchy.  It returns the number of that form.  If not found, it returns 0.
 */
function getFirstFieldFormNum (field) { // eslint-disable-line no-unused-vars
  'use strict'

  const divElem = document.querySelector('.hierarchical-search')

  const childDivs = divElem.querySelectorAll(':scope > div') // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

  const selectedformat = getSelectedFormat()

  let formatFound = false

  // This will traverse a hierarchy for each possible output format
  for (let i = 1; i < childDivs.length; i++) {
    let curfmt

    // If the div has a "-hierarchy" ID, we're at the root, so we can update the format name
    if (typeof childDivs[i].id !== 'undefined' && childDivs[i].id && childDivs[i].id.includes('-hierarchy')) {
      curfmt = '' + childDivs[i].id.split('-').shift()
    }

    if (curfmt === selectedformat) {
      formatFound = true
      const formNum = getFirstFieldFormNumHelper(childDivs[i], field, 0)
      if (formNum > 0) {
        return formNum
      }
    }
  }

  if (!formatFound) {
    console.error('Selected format', selectedformat, 'not found among the search hierarchies.')
  }

  return 0
}

/**
 * Recursive helper to getFirstFieldFormNum
 */
function getFirstFieldFormNumHelper (divElem, field, formNum) {
  'use strict'

  const childDivs = divElem.querySelectorAll(':scope > div') // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

  // Always traverse 1 less, because there's always an empty trailing div tag
  const numChildren = (childDivs.length - 1)

  // This gets inputs belonging to the parent
  const childInputs = divElem.childNodes

  for (let i = 0; i < childInputs.length; i++) {
    if (typeof childInputs[i].name !== 'undefined' && childInputs[i].name) {
      if (childInputs[i].name.includes('-fld')) {
        formNum += 1
        if (childInputs[i].value === field) {
          return formNum
        }
      }
    }
  }

  // Recurse
  // Always traverse 1 less, because there's always an empty trailing div tag
  for (let i = 0; i < numChildren; i++) {
    formNum = getFirstFieldFormNumHelper(childDivs[i], field, formNum)
    if (formNum > 0) {
      return formNum
    }
  }

  return 0
}

/**
 * This returns the number of field forms + the number of field form groups contained in the selected root group. Returns a hash of counts for "members" and "forms".  Members is the number of groups and an field forms that are children of the root group.  Forms is the number of search forms (excluding groups) that are children of the root group.
 */
function getRootGroupSize () { // eslint-disable-line no-unused-vars
  'use strict'

  const divElem = document.querySelector('.hierarchical-search')

  const childDivs = divElem.querySelectorAll(':scope > div') // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

  const selectedformat = getSelectedFormat()

  // This will traverse a hierarchy for each possible output format
  for (let i = 1; i < childDivs.length; i++) {
    const groupSize = getRootGroupSizeHelper(childDivs[i], selectedformat)
    if (groupSize.members > 0) {
      return groupSize
    }
  }
  return {
    members: 0,
    forms: 0
  }
}

/**
 * Recursive helper to getRootGroupSize
 */
function getRootGroupSizeHelper (divElem, selectedformat) {
  'use strict'

  const groupSize = {
    members: 0,
    forms: 0
  }

  let curfmt
  // If the div has a "-hierarchy" ID, we're at the root, so we can update the format name
  if (typeof divElem.id !== 'undefined' && divElem.id && divElem.id.includes('-hierarchy')) {
    curfmt = '' + divElem.id.split('-').shift()
  } else {
    console.error('Search form hierarchy malformed.')
  }

  if (curfmt !== selectedformat) {
    return groupSize
  }
  const childDivs = divElem.querySelectorAll(':scope > div')

  // Always traverse 1 less, because there's always an empty trailing div tag
  groupSize.members = childDivs.length - 1

  // This gets inputs belonging to the parent
  for (let i = 0; i < childDivs.length; i++) {
    const childInputs = childDivs[i].childNodes
    for (let j = 0; j < childInputs.length; j++) {
      if (typeof childInputs[j].name !== 'undefined' && childInputs[j].name) {
        if (childInputs[j].name.includes('-fld')) {
          groupSize.forms += 1
        }
      }
    }
  }

  return groupSize
}

/**
 * This method inserts a search field form as the new first child of the root group.
 */
function insertFirstSearch (query, format) { // eslint-disable-line no-unused-vars
  const divElem = document.querySelector('.hierarchical-search')
  const childDivs = divElem.querySelectorAll(':scope > div') // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div
  const selectedformat = getSelectedFormat()

  // This will traverse a hierarchy for each possible output format
  for (let i = 1; i < childDivs.length; i++) {
    let curfmt
    if (typeof childDivs[i].id !== 'undefined' && childDivs[i].id && childDivs[i].id.includes('-hierarchy')) {
      curfmt = '' + childDivs[i].id.split('-').shift()
    }

    if (selectedformat === curfmt) {
      const childNodes = childDivs[i].childNodes
      // This assumes a grouptype select node comes before a search form
      for (let j = 1; j < childNodes.length; j++) {
        if (typeof childNodes[j].className !== 'undefined' && childNodes[j].className) {
          if (childNodes[j].className === 'level-indent') {
            // Create a new query div
            const queryDiv = createSearchFieldFormDiv(format, query, rootGroup.searches[format].tree.queryGroup)

            // Prepend the new query before the grouptype node we just found
            childDivs[i].insertBefore(queryDiv, childNodes[j])

            return
          }
        }
      }
    }
  }
}

function attachInfoTooltip (triggerOnHoverElem, tooltipContentElem) {
  triggerOnHoverElem.onmouseover = function () {
    const rect = triggerOnHoverElem.getBoundingClientRect()
    const iconposx = rect.right
    const iconposy = rect.bottom
    const tooltipposx = iconposx - tooltipContentElem.offsetWidth
    const tooltipposy = iconposy
    tooltipContentElem.style.position = 'absolute'
    tooltipContentElem.style.top = tooltipposy + 'px'
    tooltipContentElem.style.left = tooltipposx + 'px'
  }
}
