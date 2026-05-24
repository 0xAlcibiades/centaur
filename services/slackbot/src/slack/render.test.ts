import { afterEach, describe, expect, it } from 'bun:test'
import { linkifyGithubFileRefs, renderMarkdownBlocks } from './render'

const ORIGINAL_BASE_URL = process.env.CENTAUR_GITHUB_FILE_LINK_BASE_URL

afterEach(() => {
  if (ORIGINAL_BASE_URL === undefined) {
    delete process.env.CENTAUR_GITHUB_FILE_LINK_BASE_URL
  } else {
    process.env.CENTAUR_GITHUB_FILE_LINK_BASE_URL = ORIGINAL_BASE_URL
  }
})

describe('linkifyGithubFileRefs', () => {
  it('links inline repo paths to GitHub blobs', () => {
    process.env.CENTAUR_GITHUB_FILE_LINK_BASE_URL = 'https://github.com/leanxyz/livermore/blob/main'

    const linked = linkifyGithubFileRefs('See `apps/web/src/foo.ts:12-14` for details.')

    expect(linked).toBe(
      'See <https://github.com/leanxyz/livermore/blob/main/apps/web/src/foo.ts#L12-L14|apps/web/src/foo.ts:12-14> for details.'
    )
  })

  it('normalizes sandbox workspace paths', () => {
    process.env.CENTAUR_GITHUB_FILE_LINK_BASE_URL =
      'https://github.com/leanxyz/livermore/blob/main/'

    const linked = linkifyGithubFileRefs('Edited `/home/agent/workspace/apps/web/src/foo.tsx:7`.')

    expect(linked).toContain(
      '<https://github.com/leanxyz/livermore/blob/main/apps/web/src/foo.tsx#L7|/home/agent/workspace/apps/web/src/foo.tsx:7>'
    )
  })

  it('does not rewrite fenced code blocks or already generated inline links twice', () => {
    process.env.CENTAUR_GITHUB_FILE_LINK_BASE_URL = 'https://github.com/leanxyz/livermore/blob/main'

    const linked = linkifyGithubFileRefs(
      ['`apps/web/src/foo.ts`', '```', 'apps/web/src/bar.ts', '```'].join('\n')
    )

    expect(linked).toContain(
      '<https://github.com/leanxyz/livermore/blob/main/apps/web/src/foo.ts|apps/web/src/foo.ts>'
    )
    expect(linked).toContain('```\napps/web/src/bar.ts\n```')
    expect(linked).not.toContain(']([apps/web')
  })

  it('unwraps text fences that are file lists so Slack can click links', () => {
    process.env.CENTAUR_GITHUB_FILE_LINK_BASE_URL = 'https://github.com/leanxyz/livermore/blob/main'

    const linked = linkifyGithubFileRefs(
      [
        '```text',
        '1  12152  apps/web/src/lib/enrichment/teams.ts',
        '2  4290   rust/ws/crates/ws-sports/src/bridge.rs',
        '```'
      ].join('\n')
    )

    expect(linked).not.toContain('```')
    expect(linked).toContain(
      '<https://github.com/leanxyz/livermore/blob/main/apps/web/src/lib/enrichment/teams.ts|apps/web/src/lib/enrichment/teams.ts>'
    )
    expect(linked).toContain(
      '<https://github.com/leanxyz/livermore/blob/main/rust/ws/crates/ws-sports/src/bridge.rs|rust/ws/crates/ws-sports/src/bridge.rs>'
    )
  })

  it('prefers explicit per-message base url over the environment fallback', () => {
    process.env.CENTAUR_GITHUB_FILE_LINK_BASE_URL = 'https://github.com/leanxyz/livermore/blob/main'

    const linked = linkifyGithubFileRefs('See `apps/web/src/foo.ts:12`.', {
      githubFileLinkBaseUrl: 'https://github.com/leanxyz/livermore/blob/feature-branch'
    })

    expect(linked).toBe(
      'See <https://github.com/leanxyz/livermore/blob/feature-branch/apps/web/src/foo.ts#L12|apps/web/src/foo.ts:12>.'
    )
  })

  it('converts existing markdown links to Slack native links', () => {
    process.env.CENTAUR_GITHUB_FILE_LINK_BASE_URL = 'https://github.com/leanxyz/livermore/blob/main'

    const linked = linkifyGithubFileRefs(
      '[apps/web/src/foo.ts](https://github.com/leanxyz/livermore/blob/main/apps/web/src/foo.ts)'
    )

    expect(linked).toBe(
      '<https://github.com/leanxyz/livermore/blob/main/apps/web/src/foo.ts|apps/web/src/foo.ts>'
    )
  })

  it('leaves markdown unchanged when no base url is configured', () => {
    delete process.env.CENTAUR_GITHUB_FILE_LINK_BASE_URL

    expect(linkifyGithubFileRefs('See `apps/web/src/foo.ts:12`.')).toBe(
      'See `apps/web/src/foo.ts:12`.'
    )
  })
})

describe('renderMarkdownBlocks', () => {
  it('applies GitHub file links before rendering markdown blocks', () => {
    process.env.CENTAUR_GITHUB_FILE_LINK_BASE_URL = 'https://github.com/leanxyz/livermore/blob/main'

    const blocks = renderMarkdownBlocks('1. `apps/web/src/foo.ts` — 10 lines')

    expect(blocks[0]?.text).toContain(
      '<https://github.com/leanxyz/livermore/blob/main/apps/web/src/foo.ts|apps/web/src/foo.ts>'
    )
  })
})
