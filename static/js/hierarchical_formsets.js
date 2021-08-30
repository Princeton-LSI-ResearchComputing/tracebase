/* Exported functions:
 *   appendSearchQuery
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
//         queryGroup: []
//       }
//     },
//     ...
//   }
// }
// Linting is disabled for the disallowance of 'no-var' because let and const don't work here
var rootGroup = {} // eslint-disable-line no-var
var formErrLabel // eslint-disable-line no-var

function init (rootGroup) { // eslint-disable-line no-unused-vars
  globalThis.rootGroup = rootGroup
  globalThis.formErrLabel = document.getElementById('formerror')
}

function appendSearchQuery (element, query) { // eslint-disable-line no-unused-vars
  'use strict'

  const myDiv = document.createElement('div')
  addFormatSelectList(myDiv, query)
  element.appendChild(myDiv)

  for (const templateId of Object.keys(query.searches)) {
    appendInnerSearchQuery(element, templateId, query.searches[templateId].tree)
  }
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

  if (!isRoot) {
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
    if (isInit) {
      const keyname = clones[i].name.split('-').pop()
      query[keyname] = copyQuery[keyname]
      clones[i].value = copyQuery[keyname]

      // If this isn't the hidden pos field and there is no value, push an error
      if (keyname !== 'pos' && copyQuery[keyname] === '') {
        errors.push(' * This is a required field.')
      }
    } else {
      query[clones[i].name] = clones[i].value
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

    const size = parentGroup.queryGroup.length
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
      parentNode.queryGroup.push(subQuery)
      appendInnerSearchQuery(element, templateId, subQuery, copyQueryArray[i], parentNode, false)
    } else {
      console.error('Unknown node type at index ' + i + ': ', copyQueryArray[i].type)
    }
  }
}

// This method has 2 functions:
//   1. It renames DOM object IDs of the input form elements to indicate a serial form number in the format Django expects.  It also updates 1 meta form element that indicates the total number of forms.
//   2. If saves each leaf's hierarchical path in a hidden input element named "pos".  The path is in the form of index.index.index... where <index> is the child index.  The single value (all or any) of inner nodes is saved in the pathin the form index-all.index-any.index, e.g. "0-all-0-any.0".
// This method takes the outer DOM object that contains all the forms
function saveSearchQueryHierarchy (divElem) { // eslint-disable-line no-unused-vars
  'use strict'

  const childDivs = divElem.querySelectorAll(':scope > div') // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

  const selectedformat = getSelectedFormat(childDivs[0])

  let total = 0

  // This will traverse a a hierarchy for each possible output format
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
  let posElem
  for (let i = 0; i < childInputs.length; i++) {
    if (typeof childInputs[i].name !== 'undefined' && childInputs[i].name) {
      if (childInputs[i].name.includes('-pos')) {
        isForm = true
        count++
        posElem = childInputs[i]
      } else if (childInputs[i].name.includes('grouptype') && childInputs[i].value === 'any') {
        isAll = false
      }
    }
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
    for (let i = 0; i < childInputs.length; i++) {
      if (typeof childInputs[i].name !== 'undefined' && childInputs[i].name) {
        // Replace (e.g. "form-0-val" or "form-__prefix__-val") with "form-<count>-val"
        const re = /-0-|-__prefix__-/
        const replacement = '-' + (count - 1) + '-'
        if (childInputs[i].for) childInputs[i].for = childInputs[i].for.replace(re, replacement)
        if (childInputs[i].id) {
          const tmp = childInputs[i].id
          const newid = tmp.replace(re, replacement)
          childInputs[i].id = newid
        }
        if (childInputs[i].name) childInputs[i].name = childInputs[i].name.replace(re, replacement)
      }
    }
  } else {
    if (isAll) {
      path += '-all'
    } else {
      path += '-any'
    }
  }

  // Recurse
  // Always traverse 1 less, because there's always an empty trailing div tag
  for (let i = 0; i < numChildren; i++) {
    count = saveSearchQueryHierarchyHelper(childDivs[i], path, count, i)
  }

  return count
}
