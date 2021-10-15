/* Exported functions:
 *   saveSearchQueryHierarchy
 *   initializeExistingSearchQuery
 *   init
 * These methods must be called from javascript in a template after DOM content has loaded.
 * initializeExistingSearchQuery should be conditionally called based on the existence of a previous search.
 * saveSearchQueryHierarchy should be called upon submit.
 */

// Globals
const minuspngpath = '/static/images/minus.png'
const pluspngpath = '/static/images/plus.png'
const pluspluspngpath = '/static/images/plusplus.png'

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

function init (rootGroup, ncmpChoices, fldTypes) { // eslint-disable-line no-unused-vars
  globalThis.rootGroup = rootGroup
  globalThis.ncmpChoices = ncmpChoices
  globalThis.fldTypes = fldTypes
  globalThis.formErrLabel = document.getElementById('formerror')
}

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

// This method dynamically adds a child form to the hierarchical form structure.
//   element [required] is an existing DOM object.
//   templateId indicates the hierarchy to which a search query is being added.
//   query [required] is either an child object node that is being added to a data structure that tracks the hierarchy, or it is an existing sibling node after which a sibling is being added (depending on the value of 'afterMode').
//   copyQuery [optional] (if defined) is a node object used to reconstruct the form hierarchy when results are loaded.
//   parentGroup [optional] is the parent object node of the hierarchy-tracking data structure used to determine where a sibling is to be inserted or a child node is to be appended (depending on the value of 'afterMode').  Root is assumed if not supplied.
//   afterMode [optional] determines whether a sibling will be created & inserted after query (if true) or if query will be appended as a child to parentGroup (if false).  Default = false.
function appendInnerSearchQuery (element, templateId, query, copyQuery, parentGroup, afterMode) {
  'use strict'

  let undef

  let isInit = false
  if (typeof copyQuery !== 'undefined' || copyQuery) {
    isInit = true
  }

  if (typeof afterMode === 'undefined') {
    afterMode = false
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

  const myDiv = document.createElement('div')
  if (!isRoot) {
    myDiv.className = 'level-indent'
  } else {
    const templatename = templateId + '-hierarchy'
    myDiv.id = templatename
    if (isHidden) {
      myDiv.style = 'display:none;'
    }
    myDiv.appendChild(document.createTextNode('DEBUG(' + templateId + '): '))
  }

  let isGroup = false

  if (('' + query.type) === 'group') {
    isGroup = true
    addGroupSelectList(myDiv, query, copyQuery, isInit)
  } else if (('' + query.type) === 'query') {
    addSearchFieldForm(myDiv, query, copyQuery, isInit, templateId)
  } else {
    formErrLabel.innerHTML = 'Error: Unrecognized query type: ' + query.type
  }

  if (!isRoot && !query.static) {
    addRemoveButton(myDiv, query, parentGroup)
  }

  if (afterMode) {
    element.after(myDiv)
  } else {
    element.appendChild(myDiv)
  }

  if (isGroup) {
    // Initialization using a copied rootgroup adds items one at a time, so don't pre-add empties.
    if (!isInit) {
      // Add a couple queries to start off
      const subQuery = {
        type: 'query',
        val: ''
      }
      query.queryGroup.push(subQuery)
      appendInnerSearchQuery(myDiv, templateId, subQuery, undef, query)

      // If this isn't the root, append a second query form
      if (!isRoot) {
        const subQuery2 = {
          type: 'query',
          val: ''
        }
        query.queryGroup.push(subQuery2)
        appendInnerSearchQuery(myDiv, templateId, subQuery2, undef, query)
      }

      // Not exactly sure why, but after adding inner elements to a group, an empty div is needed to make future dynamically-added form elements to be correctly created.  I did this based on the template post I followed that had a static empty div just inside where the dynamic content was being created, when stuff I was adding wasn't working right and it seems to have fixed it.
      myDiv.append(document.createElement('div'))
    }

    // Initialization using a copied rootgroup adds items one at a time, so don't add the follow-up + and ++ buttons.  This way, the individually eppended inner forms don't go under these buttons.  This means that the initializing function must add these manually.
    if (!isRoot && !isInit) {
      addQueryAndGroupAddButtons(myDiv, query, parentGroup, templateId)
    }
  } else {
    addQueryAndGroupAddButtons(myDiv, query, parentGroup, templateId)
  }

  // Return the div that was created
  return myDiv
}

function addSearchFieldForm (myDiv, query, copyQuery, isInit, templateId) {
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
  var valClone // eslint-disable-line no-var
  for (let i = 0; i < clones.length; i++) {
    // If an invalid form was previously submitted, we will need to present errors
    const errors = []

    // Dismiss any previous error (that was previously presented and prevented)
    clones[i].addEventListener('click', function (event) {
      formErrLabel.innerHTML = ''
    })

    // Keep the value of the hierarchy structure up to date when the user changes the form value
    clones[i].addEventListener('change', function (event) {
      query[clones[i].name] = event.target.value
    })

    // Initialize the value in the hierarchy with the default
    const keyname = clones[i].name.split('-').pop()
    if (isInit) {
      query[keyname] = copyQuery[keyname]
      clones[i].value = copyQuery[keyname]
      iclones[i].static = copyQuery.static

      // If this isn't the hidden pos field and there is no value, push an error
      if (keyname !== 'pos' && copyQuery[keyname] === '') {
        errors.push(' * This is a required field.')
      }

      if (keyname === 'fld') {
        fldInitVal = copyQuery[keyname]
      } else if (keyname === 'ncmp') {
        ncmpInitVal = copyQuery[keyname]
      }
    } else {
      if (typeof query[keyname] !== 'undefined' && query[keyname]) {
        console.log("Initializing search form value for field: ", keyname,":", query[keyname])
        clones[i].value = query[keyname]
      } else {
        console.log("Storing search form value for field: ", keyname,": as:", clones[i].value, "into the qry object")
        query[clones[i].name] = clones[i].value
      }
      if (query.static) {
        clones[i].disabled = true
      }

      if (keyname === 'fld') {
        fldInitVal = clones[i][0].value
      } else if (keyname === 'ncmp') {
        ncmpInitVal = clones[i][0].value
      }
    }

    if (keyname === 'fld') {
      fldClone = clones[i]
    } else if (keyname === 'ncmp') {
      ncmpClone = clones[i]
    } else if (keyname === 'val') {
      valClone = clones[i]
      // Hide the val text field
      clones[i].style = 'display:none;'
    }

    // Add this row to the HTML form
    myDiv.appendChild(clones[i])
    myDiv.appendChild(document.createTextNode(' '))

    // If there were any errors, create an error label
    // For some reason, this was a nice tooltip in an earlier version (f9c2cac151f9909380022cea8b7a40a5f0e72a4e), but doesn't work automatically in the latest version
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

  // Initialize the ncmp choices and val field(s)
  updateNcmpChoices(fldInitVal, ncmpClone, templateId)
  ncmpClone.value = ncmpInitVal
  const valFields = updateValFields(fldInitVal, ncmpInitVal, valClone, myDiv, templateId)

  // Keep the ncmp select list choices updated to reflect the fld value
  fldClone.addEventListener('change', function (event) {
    updateNcmpChoices(event.target.value, ncmpClone, rootGroup.selectedtemplate)
    updateValFields(event.target.value, ncmpClone.value, valClone, myDiv, rootGroup.selectedtemplate, valFields)
  })

  // Keep the val fields updated to also reflect the ncmp value (currently only affected by values isnull and not_isnull)
  ncmpClone.addEventListener('change', function (event) {
    updateValFields(fldClone.value, event.target.value, valClone, myDiv, rootGroup.selectedtemplate, valFields)
  })
}

function updateValFields (fldInitVal, ncmpInitVal, valClone, myDiv, templateId, valFields) {
  const dbFieldType = getDBFieldType(templateId, fldInitVal)
  const dbFieldChoices = getDBEnumFieldChoices(templateId, fldInitVal)

  let isAddMode = false
  // Create custom field for the val input, to be shown/hidden based on the other select-list selections
  if (typeof valFields === 'undefined' || !valFields) {
    isAddMode = true
    valFields = {}

    // For string and number fld types when ncmp is not (isnull or not_isnull)
    valFields.valTextBox = document.createElement('input')
    valFields.valTextBox.placeholder = valClone.placeholder
    valFields.valTextBox.value = valClone.value

    // For string, number, and enumeration fld types when ncmp is (isnull or not_isnull)
    valFields.valHiddenBox = document.createElement('input')
    valFields.valHiddenBox.style = 'display:none;'
    valFields.valHiddenBox.placeholder = valClone.placeholder
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
    console.log('Hiding all (but hidden field) for null comparison ncmp')
    valFields.valTextBox.style = 'display:none;'
    valFields.valSelectList.style = 'display:none;'
  } else if (dbFieldType === 'string' || dbFieldType === 'number') {
    valClone.value = valFields.valTextBox.value
    console.log('Hiding all but text box field for string/number fld type')
    valFields.valTextBox.style = ''
    valFields.valSelectList.style = 'display:none;'
    if (dbFieldType === 'string') {
      valFields.valTextBox.placeholder = 'search term'
    } else {
      valFields.valTextBox.placeholder = 'search value'
    }
  } else if (dbFieldType === 'enumeration') {
    updateValEnumSelectList(valFields.valSelectList, dbFieldChoices)
    valClone.value = valFields.valSelectList.value
    console.log('Hiding all but select list for enumeration fld type')
    valFields.valTextBox.style = 'display:none;'
    valFields.valSelectList.style = ''
  } else {
    console.error('Undetermined database field type for template/field-path:', templateId, fldInitVal, 'Unable to determine val form field type.')
    valFields.valTextBox.style = ''
    valFields.valSelectList.style = ''
  }

  if (isAddMode) {
    myDiv.appendChild(valFields.valTextBox)
    myDiv.appendChild(valFields.valHiddenBox)
    myDiv.appendChild(valFields.valSelectList)
    console.log('Created visible val fields for type:', dbFieldType)
  } else {
    console.log('Updated visible val fields for type:', dbFieldType)
  }

  return valFields
}

function updateValEnumSelectList (valSelectList, dbFieldChoices) {
  const arrOptions = []
  for (let i = 0; i < dbFieldChoices.length; i++) {
    arrOptions.push('<option value="' + dbFieldChoices[i][0] + '">' + dbFieldChoices[i][1] + '</option>')
  }
  const theSame = isValEnumSelectListTheSame(valSelectList, dbFieldChoices)
  if (!theSame) {
    if (dbFieldChoices.length > 0) {
      valSelectList.innerHTML = arrOptions.join('')
      valSelectList.value = dbFieldChoices[0][0]
    } else {
      valSelectList.innerHTML = ''
    }
  }
}

// The purpose of this is to re-use the enumeration's select list if the user has switched away from an enumeration
// field and then back to the same enumeration field.  This preserves the previous selection.  One limitation of this
// approach is that it doesn't preserve selections when switching between different enumeration fields (but string/
// number values aren't preserved either, so NBD).
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

function getDBFieldType (templateId, fldInitVal) {
  return fldTypes[templateId][fldInitVal].type
}

function getDBEnumFieldChoices (templateId, fldInitVal) {
  return fldTypes[templateId][fldInitVal].choices
}

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

  if (choices.length > 0) {
    const arrOptions = []
    for (let i = 0; i < choices.length; i++) {
      arrOptions.push('<option value="' + choices[i][0] + '">' + choices[i][1] + '</option>')
    }
    ncmpSelectElem.innerHTML = arrOptions.join('')
    ncmpSelectElem.value = choices[0][0]
  }
}

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

function addFormatSelectList (myDiv, query, copyQuery) {
  // Initialize the value in the hierarchy with the default
  if (typeof copyQuery !== 'undefined' || copyQuery) {
    query.selectedtemplate = copyQuery.selectedtemplate
  }

  updateBrowseLink(query.selectedtemplate)

  // Create a group type select list
  const select = document.createElement('select')
  select.name = 'fmt'
  for (const key of Object.keys(query.searches)) {
    const option = document.createElement('option')
    option.value = key
    option.text = query.searches[key].name
    select.appendChild(option)
  }
  select.value = query.selectedtemplate

  // Use a change as an opportunity to dismiss previous errors
  // And keep the selected value up to date in the object
  select.addEventListener('change', function (event) {
    formErrLabel.innerHTML = ''
    query.selectedtemplate = event.target.value
    showOutputFormatSearch(query.selectedtemplate)
    updateBrowseLink(query.selectedtemplate)
  })

  // Put descriptive text in front of the select list
  const label1 = document.createElement('label')
  label1.innerHTML = 'Output Format: '

  // Add the group select list to the DOM
  myDiv.appendChild(label1)
  myDiv.appendChild(document.createTextNode(' '))
  myDiv.appendChild(select)
}

function addGroupSelectList (myDiv, query, copyQuery, isInit) {
  // Initialize the value in the hierarchy with the default
  if (isInit) {
    query.val = copyQuery.val
  }

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
  select.value = query.val
  if (query.static || ((typeof copyQuery !== 'undefined' || copyQuery) && copyQuery.static)) {
    select.disabled = true
  } else {
    console.warn("Group",query.val,"was not static:", query.static)
  }

  // Use a change as an opportunity to dismiss previous errors
  select.addEventListener('change', function (event) {
    formErrLabel.innerHTML = ''
    query.val = event.target.value
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

function addRemoveButton (myDiv, query, parentGroup) {
  const rmBtn = document.createElement('a')
  rmBtn.href = 'javascript:void(0)'
  const btnImg = document.createElement('img')
  btnImg.src = minuspngpath
  rmBtn.appendChild(btnImg)
  rmBtn.addEventListener('click', function (event) {
    formErrLabel.innerHTML = ''

    var size = 0
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

function addQueryAndGroupAddButtons (myDiv, query, parentGroup, templateId) {
  let undef

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
    appendInnerSearchQuery(event.target.parentNode.parentNode, templateId, sibQuery, undef, parentGroup, true)
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
    appendInnerSearchQuery(event.target.parentNode.parentNode, templateId, sibGroup, undef, parentGroup, true)
  })
  myDiv.appendChild(document.createTextNode(' '))
  myDiv.appendChild(grpbtn)
}

// This method is for reconstructing the hierarchical forms on the results page
//   element is the DOM object to which the forms will be added
//   initQuery is the hierarchical form data structure that the reconstruction is based on.
function initializeExistingSearchQuery (element, initQuery) { // eslint-disable-line no-unused-vars
  'use strict'

  const myDiv = document.createElement('div')
  addFormatSelectList(myDiv, rootGroup, initQuery)
  element.appendChild(myDiv)

  for (const templateId of Object.keys(initQuery.searches)) {
    // Create the root object
    const childDiv = appendInnerSearchQuery(element, templateId, rootGroup.searches[templateId].tree, initQuery.searches[templateId].tree)

    initializeExistingSearchQueryHelper(childDiv, templateId, rootGroup.searches[templateId].tree, initQuery.searches[templateId].tree.queryGroup)

    // Not exactly sure why, but after adding inner elements to a group, an empty div is needed to make future dynamically-added form elements to be correctly created.  I did this based on the template post I followed that had a static empty div just inside where the dynamic content was being created, when stuff I was adding wasn't working right and it seems to have fixed it.
    childDiv.append(document.createElement('div'))
  }
}

// This is a recursive method called by initializeExistingSearchQuery.  It traverses the copyQueryArray data structure.  Recursion happens on inner nodes of the hierarchical data structure.
//   element is the DOM object to which the forms will be added
//   templateId indicates the hierarchy to which a search query is being added.
//   parentNode is a reference to the parent of the current copyQueryArray object.
//   copyQueryArray is a sub-tree of the hierarchical form data structure.
function initializeExistingSearchQueryHelper (element, templateId, parentNode, copyQueryArray) {
  'use strict'

  for (let i = 0; i < copyQueryArray.length; i++) {
    if (copyQueryArray[i].type === 'group') {
      const subGroup = {
        type: 'group',
        val: copyQueryArray[i].val,
        queryGroup: []
      }
      subGroup.static = copyQueryArray[i].static
      parentNode.queryGroup.push(subGroup)
      const childDiv = appendInnerSearchQuery(element, templateId, subGroup, copyQueryArray[i], parentNode, false)
      // Recurse
      initializeExistingSearchQueryHelper(childDiv, templateId, subGroup, copyQueryArray[i].queryGroup)

      // Not exactly sure why, but after adding inner elements to a group, an empty div is needed to make future dynamically-added form elements to be correctly created.  I did this based on the template post I followed that had a static empty div just inside where the dynamic content was being created, when stuff I was adding wasn't working right and it seems to have fixed it.
      childDiv.append(document.createElement('div'))

      addQueryAndGroupAddButtons(childDiv, subGroup, parentNode, templateId)
    } else if (copyQueryArray[i].type === 'query') {
      const subQuery = {
        type: 'query'
      }
      subQuery.static = copyQueryArray[i].static
      parentNode.queryGroup.push(subQuery)
      appendInnerSearchQuery(element, templateId, subQuery, copyQueryArray[i], parentNode, false)
    } else {
      console.error('Unknown node type at index ' + i + ': ', copyQueryArray[i].type)
    }
  }
}

function initializeRootSearchQuery (element) { // eslint-disable-line no-unused-vars
  'use strict'

  let undef

  const myDiv = document.createElement('div')
  addFormatSelectList(myDiv, rootGroup)
  element.appendChild(myDiv)

  for (const templateId of Object.keys(rootGroup.searches)) {
    let isHidden = false
    if (rootGroup.selectedtemplate !== templateId) {
      isHidden = true
    }

    // Create the group select list
    const groupDiv = document.createElement('div')
    const templatename = templateId + '-hierarchy'
    groupDiv.id = templatename
    if (isHidden) {
      groupDiv.style = 'display:none;'
    }
    addGroupSelectList(groupDiv, rootGroup.searches[templateId].tree, undef, false)

    initializeRootSearchQueryHelper(groupDiv, templateId, rootGroup.searches[templateId].tree, rootGroup.searches[templateId].tree.queryGroup)

    element.appendChild(groupDiv)
    // Not exactly sure why, but after adding inner elements to a group, an empty div is needed to make future dynamically-added form elements to be correctly created.  I did this based on the template post I followed that had a static empty div just inside where the dynamic content was being created, when stuff I was adding wasn't working right and it seems to have fixed it.
    groupDiv.append(document.createElement('div'))
  }
}

function initializeRootSearchQueryHelper (element, templateId, parentNode, queryGroup) {
  'use strict'

  let undef

  for (let i = 0; i < queryGroup.length; i++) {
    if (queryGroup[i].type === 'group') {
      // Create the group select list
      const groupDiv = document.createElement('div')
      groupDiv.className = 'level-indent'
      addGroupSelectList(groupDiv, queryGroup[i], undef, false)

      if (!queryGroup[i].static && !parentNode.static) {
        addRemoveButton(groupDiv, queryGroup[i], parentNode)
      }
      console.log("Recursing for template", templateId)
      initializeRootSearchQueryHelper(groupDiv, templateId, queryGroup[i], queryGroup[i].queryGroup)

      // Not exactly sure why, but after adding inner elements to a group, an empty div is needed so that future dynamically-added form elements are correctly created.  I did this based on the template post I followed that had a static empty div just inside where the dynamic content was being created, when stuff I was adding wasn't working right and it seems to have fixed it.
      groupDiv.append(document.createElement('div'))

      if (!parentNode.static) {
        addQueryAndGroupAddButtons(groupDiv, queryGroup[i], parentNode, templateId)
      }

      element.appendChild(groupDiv)
    } else if (queryGroup[i].type === 'query') {
      const queryDiv = document.createElement('div')
      queryDiv.className = 'level-indent'
      addSearchFieldForm(queryDiv, queryGroup[i], undef, false, templateId)

      if (!queryGroup[i].static) {
        addRemoveButton(queryDiv, queryGroup[i], parentNode)
      }

      if (!parentNode.static) {
        addQueryAndGroupAddButtons(queryDiv, queryGroup[i], parentNode, templateId)
      }

      element.appendChild(queryDiv)
    } else {
      console.error('Unknown node type at index ' + i + ': ', queryGroup[i].type)
    }
  }
}

function getSelectedFormat (divElem) {
  let selectedformat = 'none'
  const childInputs = divElem.childNodes
  for (let i = 0; i < childInputs.length; i++) {
    if (typeof childInputs[i].name !== 'undefined' && childInputs[i].name) {
      if (childInputs[i].name.includes('fmt')) {
        selectedformat = '' + childInputs[i].value
      }
    }
  }
  if (selectedformat === 'none') {
    console.error('Could not get selected format')
  }
  return selectedformat
}

function getFormatName (fmt) {
  const formatName = rootGroup.searches[fmt].name
  if (formatName.includes('-') || formatName.includes('.')) {
    console.error('Format name', formatName, 'is not allowed to contain dots or dashes.')
  }
  return formatName
}

// This method has 2 functions:
//   1. It renames DOM object IDs of the input form elements to indicate a serial form number in the format Django expects.  It also updates 1 meta form element that indicates the total number of forms.
//   2. It saves each leaf's hierarchical path in a hidden input element named "pos".  The path is in the form of index.index.index... where <index> is the child index.  The single value (all or any) of inner nodes is saved in the pathin the form index-all.index-any.index, e.g. "0-all-0-any.0".
// This method takes the outer DOM object that contains all the forms
function saveSearchQueryHierarchy (divElem) { // eslint-disable-line no-unused-vars
  'use strict'

  const childDivs = divElem.querySelectorAll(':scope > div') // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

  const selectedformat = getSelectedFormat(childDivs[0])

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

// This is a recursive helper method to saveSearchQueryHierarchy.  It takes:
//   divElem - The DOM object that contains forms.
//   path - a running path string to be stored in a leaf form's hidden 'pos' field.
//   count - The serial form number used to set the form element ID to what Django expects.
//   idx - The hierarchical node index, relative to the parent's child node array.
//   selectedformat - The selected item in the fmt select list
function saveSearchQueryHierarchyHelper (divElem, path, count, idx, selectedformat) {
  'use strict'

  // If the div has a "-hierarchy" ID, we're at the root, so we can grab the output format name
  let fmt = ''
  if (typeof divElem.id !== 'undefined' && divElem.id && divElem.id.includes('-hierarchy')) {
    fmt = '' + divElem.id.split('-').shift()
  }

  const childDivs = divElem.querySelectorAll(':scope > div') // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

  // Always traverse 1 less, because there's always an empty trailing div tag
  const numChildren = (childDivs.length - 1)

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
          // Infer static form by presence of disabled attribute
          staticGroup = true
        }
      } else if (childInputs[i].name.includes('-static')) {
        stcElem = childInputs[i]
      } else if (childInputs[i].disabled) {
        // Infer static search form if any other form element is disabled
        staticForm = true
      }
      if (childInputs[i].disabled) {
        // Remove the disabled attribute so that the data submits
        childInputs[i].removeAttribute('disabled')
      }
  }
    console.log("Saving template:",selectedformat,"Input:",childInputs[i].name,"Value:",childInputs[i].value)
  }

  if (path === '') {
    const formatName = getFormatName(fmt)
    // Set up the root of the path to indicate the output format
    if (selectedformat === fmt) {
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
  for (let i = 0; i < numChildren; i++) {
    count = saveSearchQueryHierarchyHelper(childDivs[i], path, count, i)
  }

  return count
}
