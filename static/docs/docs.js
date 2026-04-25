// LavinMQ HTTP API docs renderer.
// Reads the bundled OpenAPI spec (refs already resolved server-side by
// `crystal run openapi/bundle.cr`) and renders endpoints grouped by tag,
// reusing main.css design tokens.

const SPEC_URL = 'openapi.bundle.json'

const escapeHTML = (str) => String(str ?? '')
  .replaceAll('&', '&amp;')
  .replaceAll('<', '&lt;')
  .replaceAll('>', '&gt;')
  .replaceAll('"', '&quot;')
  .replaceAll("'", '&#39;')

// Minimal markdown: paragraphs, inline code, **bold**, links.
// Spec descriptions don't use full markdown; this is intentionally tiny.
function renderMarkdown (text) {
  if (!text) return ''
  const escaped = escapeHTML(text)
  const inline = escaped
    .replaceAll(/`([^`]+)`/g, '<code>$1</code>')
    .replaceAll(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>')
    .replaceAll(/\[([^\]]+)\]\(([^)]+)\)/g,
      '<a href="$2" target="_blank" rel="noopener">$1</a>')
  return inline.split(/\n{2,}/).map((p) => `<p>${p.replaceAll('\n', '<br>')}</p>`).join('')
}

// Resolve a JSON pointer like "#/components/schemas/Foo" against the spec root.
function resolveRef (root, ref) {
  if (!ref || !ref.startsWith('#/')) return null
  return ref.slice(2).split('/').reduce((acc, key) => {
    if (acc == null) return null
    return acc[key.replaceAll('~1', '/').replaceAll('~0', '~')]
  }, root)
}

// Strip $refs to a depth so we don't render circular structures.
function dereference (root, node, seen = new Set(), depth = 0) {
  if (node == null || typeof node !== 'object' || depth > 6) return node
  if (Array.isArray(node)) return node.map((n) => dereference(root, n, seen, depth + 1))
  if (typeof node.$ref === 'string') {
    if (seen.has(node.$ref)) return { _circular: node.$ref }
    seen = new Set([...seen, node.$ref])
    const target = resolveRef(root, node.$ref)
    if (target == null) return node
    return dereference(root, target, seen, depth + 1)
  }
  const out = {}
  for (const [k, v] of Object.entries(node)) {
    out[k] = dereference(root, v, seen, depth + 1)
  }
  return out
}

const METHODS = ['get', 'post', 'put', 'patch', 'delete', 'head', 'options']

function operationId (method, path) {
  return 'op-' + method + '-' + path.replaceAll(/[^a-zA-Z0-9]+/g, '-').replace(/^-+|-+$/g, '')
}

function tagId (tag) {
  return 'tag-' + tag.replaceAll(/[^a-zA-Z0-9]+/g, '-')
}

function groupOperations (spec) {
  const groups = new Map()
  for (const [path, pathItem] of Object.entries(spec.paths || {})) {
    if (!pathItem || typeof pathItem !== 'object') continue
    for (const method of METHODS) {
      const op = pathItem[method]
      if (!op) continue
      const tags = Array.isArray(op.tags) && op.tags.length ? op.tags : ['default']
      for (const tag of tags) {
        if (!groups.has(tag)) groups.set(tag, [])
        groups.get(tag).push({ path, method, op })
      }
    }
  }
  return groups
}

function renderTOC (groups) {
  const html = []
  for (const [tag, ops] of groups) {
    html.push('<div class="docs-toc-tag">')
    html.push(`<a class="docs-toc-tag-title" href="#${tagId(tag)}">${escapeHTML(tag)}</a>`)
    html.push('<ul>')
    for (const { method, path } of ops) {
      const id = operationId(method, path)
      html.push(`<li><a href="#${id}" data-op="${id}">`)
      html.push(`<span class="docs-toc-method docs-method-${method}">${method}</span>`)
      html.push(`<span class="docs-toc-path">${escapeHTML(path)}</span>`)
      html.push('</a></li>')
    }
    html.push('</ul></div>')
  }
  return html.join('')
}

function renderParameters (params) {
  if (!params || params.length === 0) return ''
  const rows = params.map((p) => {
    const schema = p.schema || {}
    return `<tr>
      <td><code>${escapeHTML(p.name)}</code>${p.required ? '<span class="docs-required" title="required">*</span>' : ''}</td>
      <td>${escapeHTML(p.in)}</td>
      <td><code>${escapeHTML(schema.type || (schema.enum ? 'enum' : '—'))}</code></td>
      <td>${renderMarkdown(p.description || '')}</td>
    </tr>`
  }).join('')
  return `<div>
    <h4 class="docs-section-title">Parameters</h4>
    <table class="docs-table">
      <thead><tr><th>Name</th><th>In</th><th>Type</th><th>Description</th></tr></thead>
      <tbody>${rows}</tbody>
    </table>
  </div>`
}

function describeSchema (schema) {
  if (!schema || typeof schema !== 'object') return '—'
  if (schema.type === 'array') {
    return `array of ${describeSchema(schema.items || {})}`
  }
  if (schema.type) return schema.type
  if (schema.oneOf) return 'oneOf'
  if (schema.anyOf) return 'anyOf'
  if (schema.allOf) return 'allOf'
  return 'object'
}

function renderSchemaBlock (schema, root) {
  if (!schema) return '<p class="docs-empty">No schema.</p>'
  const resolved = dereference(root, schema)
  return `<pre class="docs-schema">${escapeHTML(JSON.stringify(resolved, null, 2))}</pre>`
}

function renderRequestBody (rb, root) {
  if (!rb) return ''
  const resolved = dereference(root, rb)
  const content = resolved.content || {}
  const blocks = Object.entries(content).map(([mime, media]) => {
    const meta = `<div class="docs-schema-meta">${escapeHTML(mime)}${describeSchema(media.schema) === '—' ? '' : ' &middot; ' + escapeHTML(describeSchema(media.schema))}</div>`
    return meta + renderSchemaBlock(media.schema, root)
  }).join('')
  return `<div>
    <h4 class="docs-section-title">Request body${resolved.required ? ' (required)' : ''}</h4>
    ${resolved.description ? `<div class="docs-description">${renderMarkdown(resolved.description)}</div>` : ''}
    ${blocks || '<p class="docs-empty">No content.</p>'}
  </div>`
}

function statusClass (code) {
  const s = String(code).trim()
  // Spec wildcards like "2XX", "4XX" — match leading digit.
  const lead = s.charAt(0)
  if (lead === '1') return 'docs-status-2xx'
  if (lead === '2') return 'docs-status-2xx'
  if (lead === '3') return 'docs-status-3xx'
  if (lead === '4') return 'docs-status-4xx'
  if (lead === '5') return 'docs-status-5xx'
  return ''
}

function renderResponses (responses, root) {
  if (!responses) return ''
  const rows = Object.entries(responses).map(([code, resp]) => {
    const r = dereference(root, resp)
    const content = r.content || {}
    const schemaBlocks = Object.entries(content).map(([mime, media]) => {
      const meta = `<div class="docs-schema-meta">${escapeHTML(mime)}${describeSchema(media.schema) === '—' ? '' : ' &middot; ' + escapeHTML(describeSchema(media.schema))}</div>`
      return meta + renderSchemaBlock(media.schema, root)
    }).join('')
    return `<div class="docs-response-row">
      <div class="docs-response-head">
        <span class="docs-status-pill ${statusClass(code)}">${escapeHTML(code)}</span>
        <span class="docs-response-desc">${escapeHTML(r.description || '')}</span>
      </div>
      ${schemaBlocks}
    </div>`
  }).join('')
  return `<div>
    <h4 class="docs-section-title">Responses</h4>
    ${rows}
  </div>`
}

function renderOperation ({ path, method, op }, root) {
  const id = operationId(method, path)
  const summary = op.summary ? escapeHTML(op.summary) : ''
  const description = op.description ? renderMarkdown(op.description) : ''
  const parts = []
  if (description) parts.push(`<div class="docs-description">${description}</div>`)
  parts.push(renderParameters(op.parameters || []))
  parts.push(renderRequestBody(op.requestBody, root))
  parts.push(renderResponses(op.responses, root))
  const body = parts.filter(Boolean).join('')
  return `<details class="docs-operation" id="${id}">
    <summary>
      <span class="docs-method docs-method-${method}">${method}</span>
      <span class="docs-path">${escapeHTML(path)}</span>
      <span class="docs-summary">${summary}</span>
    </summary>
    <div class="docs-operation-body">${body}</div>
  </details>`
}

function renderTagSection (tag, ops, spec) {
  const tagDef = (spec.tags || []).find((t) => t.name === tag)
  const desc = tagDef && tagDef.description ? `<p class="docs-tag-desc">${renderMarkdown(tagDef.description)}</p>` : ''
  const operations = ops.map((entry) => renderOperation(entry, spec)).join('')
  return `<section class="docs-tag-section" id="${tagId(tag)}">
    <h2>${escapeHTML(tag)}</h2>
    ${desc}
    ${operations}
  </section>`
}

function setupSearch (groups) {
  const input = document.getElementById('docs-search')
  if (!input) return
  input.addEventListener('input', () => {
    const term = input.value.trim().toLowerCase()
    document.querySelectorAll('#docs-toc li').forEach((li) => {
      const text = li.textContent.toLowerCase()
      li.style.display = !term || text.includes(term) ? '' : 'none'
    })
    document.querySelectorAll('.docs-operation').forEach((op) => {
      const text = op.textContent.toLowerCase()
      op.style.display = !term || text.includes(term) ? '' : 'none'
    })
    document.querySelectorAll('.docs-tag-section').forEach((s) => {
      const visible = s.querySelectorAll('.docs-operation').length === 0 ||
        Array.from(s.querySelectorAll('.docs-operation')).some((op) => op.style.display !== 'none')
      s.style.display = visible ? '' : 'none'
    })
  })
}

// Lightweight theme switcher mirroring static/js/layout.js's behavior.
// Persists state via the same `lmq.stateclasses` localStorage key so that
// switching to/from the management UI keeps the theme.
const ThemeKey = 'lmq.stateclasses'

function readStateClasses () {
  try {
    return new Set((window.localStorage.getItem(ThemeKey) || '').split(' ').filter(Boolean))
  } catch (_) { return new Set() }
}

function writeStateClasses (set) {
  try { window.localStorage.setItem(ThemeKey, [...set].join(' ')) } catch (_) {}
}

function applyTheme (theme) {
  const state = readStateClasses()
  state.delete('theme-light')
  state.delete('theme-dark')
  state.delete('theme-system')
  let resolved = theme
  if (theme === 'system') {
    resolved = window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
  }
  if (resolved === 'light') state.add('theme-light')
  else state.add('theme-dark')
  if (theme === 'system') state.add('theme-system')
  writeStateClasses(state)
  document.documentElement.classList.remove('theme-light', 'theme-dark')
  document.documentElement.classList.add(resolved === 'light' ? 'theme-light' : 'theme-dark')
  document.querySelectorAll('#theme-switcher button').forEach((btn) => {
    btn.classList.toggle('active', btn.dataset.theme === theme)
  })
}

function setupThemeSwitcher () {
  const buttons = document.querySelectorAll('#theme-switcher button')
  if (!buttons.length) return
  buttons.forEach((btn) => {
    btn.addEventListener('click', () => applyTheme(btn.dataset.theme))
  })
  const state = readStateClasses()
  let active = 'system'
  if (state.has('theme-system')) active = 'system'
  else if (state.has('theme-light')) active = 'light'
  else if (state.has('theme-dark')) active = 'dark'
  applyTheme(active)
}

async function loadSpec () {
  const res = await fetch(SPEC_URL, { credentials: 'same-origin' })
  if (!res.ok) throw new Error(`Failed to load ${SPEC_URL}: ${res.status}`)
  return res.json()
}

async function init () {
  setupThemeSwitcher()
  const loading = document.getElementById('docs-loading')
  let spec
  try {
    spec = await loadSpec()
  } catch (err) {
    if (loading) loading.textContent = 'Failed to load API spec: ' + err.message
    return
  }

  document.getElementById('docs-title').textContent = (spec.info && spec.info.title) || 'HTTP API'
  if (spec.info && spec.info.description) {
    document.getElementById('docs-description').innerHTML = renderMarkdown(spec.info.description)
  }
  if (spec.info && spec.info.version) {
    document.getElementById('docs-spec-version').textContent = spec.info.version
  }

  const groups = groupOperations(spec)
  document.getElementById('docs-toc').innerHTML = renderTOC(groups)

  const sections = []
  for (const [tag, ops] of groups) {
    sections.push(renderTagSection(tag, ops, spec))
  }
  document.getElementById('docs-tags').innerHTML = sections.join('')

  if (loading) loading.remove()

  setupSearch(groups)

  // Auto-open the operation referenced by the URL hash so deep links work.
  if (window.location.hash) {
    const target = document.querySelector(window.location.hash)
    if (target && target.tagName === 'DETAILS') {
      target.open = true
      target.scrollIntoView({ block: 'start' })
    }
  }
}

init()
