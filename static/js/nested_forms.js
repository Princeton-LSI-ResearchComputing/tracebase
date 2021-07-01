/* Exported functions:
 *   saveSearchQueryHierarchy
 *   initializeExistingSearchQuery
 * These methods must be called from javascript in a template after DOM content has loaded.
 * initializeExistingSearchQuery should be conditionally called based on the existence of a previous search.
 * saveSearchQueryHierarchy should be called upon submit.
 */

// Globals
const minuspngpath = '/static/images/minus.png'
const pluspngpath = '/static/images/plus.png'
const pluspluspngpath = '/static/images/plusplus.png'
// This is the default root of the form hierarchy
const rootGroup = {
  type: 'group',
  val: 'all',
  queryGroup: []
}

// This method dynamically adds a child form to the hierarchical form structure.
//   element [required] is an existing DOM object.
//   query [required] is either an child object node that is being added to a data structure that tracks the hierarchy, or it is an existing sibling node after which a sibling is being added (depending on the value of 'afterMode').
//   copyQuery [optional] (if defined) is a node object used to reconstruct the form hierarchy when results are loaded.
//   parentGroup [optional] is the parent object node of the hierarchy-tracking data structure used to determine where a sibling is to be inserted or a child node is to be appended (depending on the value of 'afterMode').  Root is assumed if not supplied.
//   afterMode [optional] determines whether a sibling will be created & inserted after query (if true) or if query will be appended as a child to parentGroup (if false).  Default = false.
function appendInnerSearchQuery (element, query, copyQuery, parentGroup, afterMode) {
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
  if (typeof parentGroup !== 'undefined' || parentGroup) {
    isRoot = false
  }

  const myDiv = document.createElement('div')
  if (!isRoot) {
    myDiv.className = 'level-indent'
  }

  let isGroup = false

  if (('' + query.type) === 'group') {
    isGroup = true

    addGroupSelectList(myDiv, query, copyQuery, isInit)
  } else if (('' + query.type) === 'query') {
    addSearchFieldForm(myDiv, query, copyQuery, isInit)
  } else {
    const label = document.getElementById('formerror')
    label.innerHTML = 'Error: Unrecognized query type: ' + query.type
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
      appendInnerSearchQuery(myDiv, subQuery, undef, query)

      // If this isn't the root, append a second query form
      if (!isRoot) {
        const subQuery2 = {
          type: 'query',
          val: ''
        }
        query.queryGroup.push(subQuery2)
        appendInnerSearchQuery(myDiv, subQuery2, undef, query)
      }

      // Not exactly sure why, but after adding inner elements to a group, an empty div is needed to make future dynamically-added form elements to be correctly created.  I did this based on the template post I followed that had a static empty div just inside where the dynamic content was being created, when stuff I was adding wasn't working right and it seems to have fixed it.
      myDiv.append(document.createElement('div'))
    }

    // Initialization using a copied rootgroup adds items one at a time, so don't add the follow-up + and ++ buttons.  This way, the individually eppended inner forms don't go under these buttons.  This means that the initializing function must add these manually.
    if (!isRoot && !isInit) {
      addQueryAndGroupAddButtons(myDiv, query, parentGroup)
    }
  } else {
    addQueryAndGroupAddButtons(myDiv, query, parentGroup)
  }

  // Return the div that was created
  return myDiv
}

function addSearchFieldForm (myDiv, query, copyQuery, isInit) {
  // Clone the form template
  const templateDiv = document.querySelector('#id-empty-form')
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
      const label = document.getElementById('formerror')
      label.innerHTML = ''
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
    const label = document.getElementById('formerror')
    label.innerHTML = ''
    query.val = event.target.value
  })

  // Put descriptive text in front of the select list
  const label1 = document.createElement('label')
  label1.innerHTML = 'Match '
  label1.htmlFor = 'grouptype'

  // Add the group select list to the DOM
  myDiv.appendChild(label1)
  myDiv.appendChild(select)
}

function addRemoveButton (myDiv, query, parentGroup) {
  const rmBtn = document.createElement('a')
  rmBtn.href = 'javascript:void(0)'
  const btnImg = document.createElement('img')
  btnImg.src = minuspngpath
  rmBtn.appendChild(btnImg)
  rmBtn.addEventListener('click', function (event) {
    const label = document.getElementById('formerror')
    label.innerHTML = ''

    const size = parentGroup.queryGroup.length
    if (size <= 1) {
      label.innerHTML = 'A match group must have at least 1 query.'
    } else {
      event.target.parentNode.parentNode.remove()
      const index = parentGroup.queryGroup.indexOf(query)
      parentGroup.queryGroup.splice(index, 1)
    }
  })
  myDiv.appendChild(document.createTextNode(' '))
  myDiv.appendChild(rmBtn)
}

function addQueryAndGroupAddButtons (myDiv, query, parentGroup) {
  let undef

  // Add query to a group (button)
  const termbtn = document.createElement('a')
  termbtn.href = 'javascript:void(0)'
  const pBtnImg = document.createElement('img')
  pBtnImg.src = pluspngpath
  termbtn.appendChild(pBtnImg)
  termbtn.addEventListener('click', function (event) {
    const label = document.getElementById('formerror')
    label.innerHTML = ''

    const sibQuery = {
      type: 'query',
      val: ''
    }
    const index = parentGroup.queryGroup.indexOf(query)
    parentGroup.queryGroup.splice(index + 1, 0, sibQuery)
    // The clicked item is the image, so to get the eclosing div, we need the grandparent
    appendInnerSearchQuery(event.target.parentNode.parentNode, sibQuery, undef, parentGroup, true)
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
    const label = document.getElementById('formerror')
    label.innerHTML = ''

    const sibGroup = {
      type: 'group',
      val: 'any',
      queryGroup: []
    }
    const index = parentGroup.queryGroup.indexOf(query)
    parentGroup.queryGroup.splice(index + 1, 0, sibGroup)
    // The clicked item is the image, so to get the eclosing div, we need the grandparent
    appendInnerSearchQuery(event.target.parentNode.parentNode, sibGroup, undef, parentGroup, true)
  })
  myDiv.appendChild(document.createTextNode(' '))
  myDiv.appendChild(grpbtn)
}

// This method is for reconstructing the hierarchical forms on the results page
//   element is the DOM object to which the forms will be added
//   initQuery is the hierarchical form data structure that the reconstruction is based on.
function initializeExistingSearchQuery (element, initQuery) { // eslint-disable-line no-unused-vars
  'use strict'

  // Create the root object
  const childDiv = appendInnerSearchQuery(element, rootGroup, initQuery[0])

  initializeExistingSearchQueryHelper(childDiv, initQuery[0].queryGroup, rootGroup)

  // Not exactly sure why, but after adding inner elements to a group, an empty div is needed to make future dynamically-added form elements to be correctly created.  I did this based on the template post I followed that had a static empty div just inside where the dynamic content was being created, when stuff I was adding wasn't working right and it seems to have fixed it.
  childDiv.append(document.createElement('div'))
}

// This is a recursive method called by initializeExistingSearchQuery.  It traverses the copyQueryArray data structure.  Recursion happens on inner nodes of the hierarchical data structure.
//   copyQueryArray is a sub-tree of the hierarchical form data structure.
//   parentNode is a reference to the parent of the current copyQueryArray object.
function initializeExistingSearchQueryHelper (element, copyQueryArray, parentNode) {
  'use strict'

  for (let i = 0; i < copyQueryArray.length; i++) {
    if (copyQueryArray[i].type === 'group') {
      const subGroup = {
        type: 'group',
        val: copyQueryArray[i].val,
        queryGroup: []
      }
      parentNode.queryGroup.push(subGroup)
      const childDiv = appendInnerSearchQuery(element, subGroup, copyQueryArray[i], parentNode, false)
      // Recurse
      initializeExistingSearchQueryHelper(childDiv, copyQueryArray[i].queryGroup, subGroup)

      // Not exactly sure why, but after adding inner elements to a group, an empty div is needed to make future dynamically-added form elements to be correctly created.  I did this based on the template post I followed that had a static empty div just inside where the dynamic content was being created, when stuff I was adding wasn't working right and it seems to have fixed it.
      childDiv.append(document.createElement('div'))

      addQueryAndGroupAddButtons(childDiv, subGroup, parentNode)
    } else if (copyQueryArray[i].type === 'query') {
      const subQuery = {
        type: 'query'
      }
      parentNode.queryGroup.push(subQuery)
      appendInnerSearchQuery(element, subQuery, copyQueryArray[i], parentNode, false)
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

  let total = 0

  // This should only traverse a single iteration (because there's only one root)
  for (let i = 0; i < childDivs.length; i++) {
    total = saveSearchQueryHierarchyHelper(childDivs[i], '', 0, 0)
  }

  const formInput = document.getElementById('id_form-TOTAL_FORMS')
  formInput.value = total
}

// This is a recursive helper method to saveSearchQueryHierarchy.  It takes:
//   divElem - The DOM object that contains forms.
//   path - a running path string to be stored in a leaf form's hidden 'pos' field.
//   count - The serial form number used to set the form element ID to what Django expects.
//   idx - The hierarchical node index, relative to the parent's child node array.
function saveSearchQueryHierarchyHelper (divElem, path, count, idx) {
  'use strict'

  // var childElems = divElem.querySelectorAll(":scope > input,select,textarea,label,div");
  const childDivs = divElem.querySelectorAll(':scope > div') // - results in only 1, even if 2 items added - I think because each input is not wrapped in a div

  // Always traverse 1 less, because there's always an empty trailing div tag
  const numChildren = (childDivs.length - 1)

  if (path === '') {
    path += idx
  } else {
    path += '.' + idx
  }

  // var childInputs = divElem.querySelectorAll("input,select,textarea");
  // This gets inputs belonging to the parent
  const childInputs = divElem.childNodes

  let isForm = false
  let isAll = true
  for (let i = 0; i < childInputs.length; i++) {
    if (typeof childInputs[i].name !== 'undefined' && childInputs[i].name) {
      if (childInputs[i].name.includes('-pos')) {
        childInputs[i].value = path
        isForm = true
        count++
      } else if (childInputs[i].name.includes('grouptype') && childInputs[i].value === 'any') {
        isAll = false
      }
    }
  }

  // If this is a form from Django formset form (otherwise it's a hierarchy control level)
  if (isForm) {
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
