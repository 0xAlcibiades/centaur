import type { AnyBlock, AnyChunk, ContextBlock, MarkdownBlock, RichTextBlock } from '@slack/types'
import { slackReplyLimits } from '../constants'

const MAX_BLOCKS = slackReplyLimits.message.maxBlocks
const MAX_MARKDOWN_CHARS = slackReplyLimits.stream.markdownChunkChars
const MAX_FALLBACK_CHARS = slackReplyLimits.text.maxFallbackChars
const MAX_STREAM_CHUNK_CHARS = slackReplyLimits.stream.markdownChunkChars
const FILE_REF_PATTERN =
  /(?<![\w./-])((?:\/home\/agent\/(?:workspace|(?:github|branches)\/[^/\s`]+\/[^/\s`]+)\/)?(?:[A-Za-z0-9_.@-]+\/)+[A-Za-z0-9_.@-]+\.(?:ts|tsx|js|jsx|mjs|cjs|rs|py|go|md|mdx|json|toml|ya?ml|css|scss|sql|sh)(?::\d+(?:-\d+)?)?)/g
const INLINE_CODE_PATTERN = /`([^`\n]+)`/g
const FENCE_PATTERN = /```([A-Za-z0-9_-]*)?\n?([\s\S]*?)```/g

export type StatusMetadata = {
  title?: string
  status?: string
  fields?: Record<string, string | number | boolean | null | undefined>
}

export function blockquoteMarkdown(text: string): string {
  return text
    .split('\n')
    .map(line => `> ${line}`)
    .join('\n')
}

/** Skip Thinking when Codex repeated the same prose in commentary and final_answer. */
export function shouldShowThinkingBlock(commentary: string, answer: string): boolean {
  const trimmedCommentary = commentary.trim()
  const trimmedAnswer = answer.trim()
  if (!trimmedCommentary) return false
  if (!trimmedAnswer) return true
  if (trimmedCommentary === trimmedAnswer) return false
  if (trimmedAnswer.includes(trimmedCommentary)) return false
  return true
}

export function thinkingContextBlock(
  commentary: string,
  opts: { heading?: boolean } = {}
): ContextBlock | null {
  const trimmed = commentary.trim()
  if (!trimmed) return null
  const maxChars = slackReplyLimits.message.thinkingContextChars
  const body =
    trimmed.length > maxChars ? `${trimmed.slice(0, maxChars - 13)}\n// truncated` : trimmed
  return {
    type: 'context',
    elements: [{ type: 'mrkdwn', text: opts.heading === false ? body : `*Thinking*\n${body}` }]
  }
}

export function renderMarkdownBlocks(markdown: string): MarkdownBlock[] {
  const normalized = linkifyGithubFileRefs(markdown).trim() || ' '
  const blocks: MarkdownBlock[] = []
  let used = 0

  for (const chunk of splitText(normalized, MAX_MARKDOWN_CHARS)) {
    if (blocks.length >= MAX_BLOCKS) break
    const remaining = MAX_MARKDOWN_CHARS - used
    if (remaining <= 0) break
    const text = chunk.slice(0, remaining)
    used += text.length
    blocks.push({ type: 'markdown', text })
  }

  return blocks
}

export function renderStatusBlock(metadata: StatusMetadata): RichTextBlock | null {
  const elements: Array<{ type: 'text'; text: string; style?: { bold?: boolean } }> = []
  if (metadata.title) {
    elements.push({ type: 'text', text: metadata.title, style: { bold: true } })
  }
  if (metadata.status) {
    if (elements.length) elements.push({ type: 'text', text: '\n' })
    elements.push({ type: 'text', text: metadata.status })
  }
  for (const [key, value] of Object.entries(metadata.fields ?? {})) {
    if (value === undefined || value === null) continue
    if (elements.length) elements.push({ type: 'text', text: '\n' })
    elements.push({ type: 'text', text: `${key}: `, style: { bold: true } })
    elements.push({ type: 'text', text: String(value) })
  }
  if (!elements.length) return null

  return {
    type: 'rich_text',
    elements: [{ type: 'rich_text_section', elements }]
  }
}

export function enforceBlockLimits(blocks: AnyBlock[]): AnyBlock[] {
  return blocks.slice(0, MAX_BLOCKS)
}

export function fallbackText(input: {
  markdown?: string
  metadata?: StatusMetadata
  fallback?: string
}): string {
  const parts = [
    input.fallback,
    input.markdown,
    input.metadata?.title,
    input.metadata?.status,
    ...Object.entries(input.metadata?.fields ?? {}).map(([key, value]) =>
      value === undefined || value === null ? '' : `${key}: ${String(value)}`
    )
  ].filter(Boolean)

  const text = parts.join('\n').replace(/\s+/g, ' ').trim() || 'Centaur update'
  return text.length > MAX_FALLBACK_CHARS ? `${text.slice(0, MAX_FALLBACK_CHARS - 1)}…` : text
}

export function markdownToStreamChunks(markdown: string): AnyChunk[] {
  return splitText(linkifyGithubFileRefs(markdown || ' '), MAX_STREAM_CHUNK_CHARS).map(text => ({
    type: 'markdown_text',
    text
  }))
}

export function linkifyGithubFileRefs(markdown: string): string {
  const baseUrl = githubFileLinkBaseUrl()
  if (!baseUrl || !markdown) return markdown
  const fenced: string[] = []
  const inline: string[] = []
  const withoutFences = markdown.replace(FENCE_PATTERN, (match, language: string, body: string) => {
    if (shouldUnwrapFileListFence(language, body)) {
      return linkifyPlainFileRefs(baseUrl, body.trimEnd())
    }
    const index = fenced.push(match) - 1
    return `@@CENTAUR_FENCE_${index}@@`
  })
  const withoutInline = withoutFences.replace(INLINE_CODE_PATTERN, (match, code: string) => {
    const url = githubUrlForFileRef(baseUrl, code.trim())
    const replacement = url ? `[${escapeMarkdownLinkText(code)}](${url})` : match
    const index = inline.push(replacement) - 1
    return `@@CENTAUR_INLINE_${index}@@`
  })
  const linked = linkifyPlainFileRefs(baseUrl, withoutInline)
  return linked
    .replace(/@@CENTAUR_INLINE_(\d+)@@/g, (_match, index: string) => inline[Number(index)] ?? '')
    .replace(/@@CENTAUR_FENCE_(\d+)@@/g, (_match, index: string) => fenced[Number(index)] ?? '')
}

function githubFileLinkBaseUrl(): string {
  return (process.env.CENTAUR_GITHUB_FILE_LINK_BASE_URL ?? '').trim().replace(/\/+$/, '')
}

function shouldUnwrapFileListFence(language: string | undefined, body: string): boolean {
  const normalizedLanguage = (language ?? '').trim().toLowerCase()
  if (normalizedLanguage && normalizedLanguage !== 'text' && normalizedLanguage !== 'plain')
    return false
  const refs = body.match(FILE_REF_PATTERN) ?? []
  return refs.length >= 2
}

function linkifyPlainFileRefs(baseUrl: string, text: string): string {
  return text.replace(FILE_REF_PATTERN, (match: string) => {
    if (match.includes('://')) return match
    const url = githubUrlForFileRef(baseUrl, match)
    return url ? `[${escapeMarkdownLinkText(match)}](${url})` : match
  })
}

function githubUrlForFileRef(baseUrl: string, rawRef: string): string | null {
  const parsed = parseFileRef(rawRef)
  if (!parsed) return null
  const path = parsed.path
    .split('/')
    .filter(Boolean)
    .map(part => encodeURIComponent(part))
    .join('/')
  const url = `${baseUrl}/${path}`
  if (!parsed.lineStart) return url
  return `${url}#L${parsed.lineStart}${parsed.lineEnd ? `-L${parsed.lineEnd}` : ''}`
}

function parseFileRef(
  rawRef: string
): { path: string; lineStart?: string; lineEnd?: string } | null {
  const trimmed = rawRef.trim()
  const match = /^(.+?)(?::(\d+)(?:-(\d+))?)?$/.exec(trimmed)
  if (!match) return null
  const rawPath = match[1]
  if (!rawPath) return null
  const path = normalizeRepoPath(rawPath)
  if (!path || !isLinkableFileRef(path)) return null
  return { path, lineStart: match[2], lineEnd: match[3] }
}

function normalizeRepoPath(path: string): string | null {
  let normalized = path.replace(/^\.\/+/, '').replace(/^\/home\/agent\/workspace\/+/, '')
  normalized = normalized.replace(/^\/home\/agent\/(?:github|branches)\/[^/]+\/[^/]+\/+/, '')
  if (normalized.startsWith('/') || normalized.startsWith('../')) return null
  return normalized
}

function isLinkableFileRef(path: string): boolean {
  FILE_REF_PATTERN.lastIndex = 0
  const matched = FILE_REF_PATTERN.test(path)
  FILE_REF_PATTERN.lastIndex = 0
  return matched
}

function escapeMarkdownLinkText(text: string): string {
  return text.replace(/([\\[\]])/g, '\\$1')
}

function splitText(input: string, maxChars: number): string[] {
  const chunks: string[] = []
  let remaining = input
  while (remaining.length > maxChars) {
    const hard = remaining.slice(0, maxChars)
    const paragraphBoundary = hard.lastIndexOf('\n\n')
    const lineBoundary = hard.lastIndexOf('\n')
    const spaceBoundary = hard.lastIndexOf(' ')
    const boundary = Math.max(paragraphBoundary, lineBoundary, spaceBoundary)
    const delimiterLength = boundary === paragraphBoundary ? 2 : boundary >= 0 ? 1 : 0
    const take = boundary > maxChars * 0.5 ? boundary + delimiterLength : maxChars
    chunks.push(remaining.slice(0, take))
    remaining = remaining.slice(take)
  }
  if (remaining) chunks.push(remaining)
  return chunks
}
