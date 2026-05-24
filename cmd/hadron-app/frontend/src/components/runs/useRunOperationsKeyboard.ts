import { useEffect, type RefObject } from 'react'
import { resolveKeyboardAction } from './runOperationsPanel.helpers'

export function useRunOperationsKeyboard(input: {
  searchRef: RefObject<HTMLInputElement | null>
  rowRefs: RefObject<Record<number, HTMLButtonElement | null>>
  visibleSequences: number[]
  activeSequence: number | null
  searchValue: string
  setSearchValue: (value: string) => void
  setActiveSequence: (sequence: number) => void
  toggleSequence: (sequence: number) => void
}) {
  const {
    searchRef,
    rowRefs,
    visibleSequences,
    activeSequence,
    searchValue,
    setSearchValue,
    setActiveSequence,
    toggleSequence,
  } = input

  useEffect(() => {
    const isEditableTarget = (target: EventTarget | null): target is HTMLElement => {
      if (!(target instanceof HTMLElement)) return false
      const tag = target.tagName
      return target.isContentEditable || tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT'
    }

    const focusSequence = (sequence: number) => {
      setActiveSequence(sequence)
      const el = rowRefs.current?.[sequence]
      if (!el) return
      el.focus()
      el.scrollIntoView({ block: 'nearest' })
    }

    const onKeyDown = (event: KeyboardEvent) => {
      const target = event.target
      const editable = isEditableTarget(target)
      const searchFocused = target === searchRef.current
      const targetIsButton = target instanceof HTMLElement && target.tagName === 'BUTTON'
      const action = resolveKeyboardAction({
        key: event.key,
        editable,
        searchFocused,
        searchValue,
        visibleSequences,
        activeSequence,
        targetIsButton,
        metaKey: event.metaKey,
        ctrlKey: event.ctrlKey,
        altKey: event.altKey,
      })

      switch (action.type) {
        case 'focus_search':
          event.preventDefault()
          searchRef.current?.focus()
          searchRef.current?.select()
          return
        case 'clear_search':
          event.preventDefault()
          setSearchValue('')
          return
        case 'blur_search':
          event.preventDefault()
          searchRef.current?.blur()
          return
        case 'focus_sequence':
          event.preventDefault()
          focusSequence(action.sequence)
          return
        case 'toggle_sequence':
          event.preventDefault()
          toggleSequence(action.sequence)
          return
        default:
          return
      }
    }

    document.addEventListener('keydown', onKeyDown)
    return () => {
      document.removeEventListener('keydown', onKeyDown)
    }
  }, [searchRef, rowRefs, visibleSequences, activeSequence, searchValue, setSearchValue, setActiveSequence, toggleSequence])
}
