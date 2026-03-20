import { useState, useEffect, type DragEvent } from 'react';
import { Plus, Search, FileCode } from 'lucide-react';
import { getBlueprintDir, listFilesInDir } from '../../api/client';
import type { FileEntry } from '../../api/types';

interface NodePaletteProps {
  onAddBlankNode: () => void;
}

// Data transferred during drag
export interface PaletteDragData {
  type: 'stageNode';
  blueprintPath: string;
  label: string;
}

function shortName(path: string): string {
  const parts = path.split(/[/\\]/);
  const file = parts[parts.length - 1] || path;
  return file.replace(/\.(yaml|yml)$/i, '');
}

export function NodePalette({ onAddBlankNode }: NodePaletteProps) {
  const [blueprints, setBlueprints] = useState<FileEntry[]>([]);
  const [search, setSearch] = useState('');
  const [loading, setLoading] = useState(false);

  // Load blueprints from the configured blueprint directory
  useEffect(() => {
    setLoading(true);
    getBlueprintDir()
      .then(dir => {
        if (!dir) return;
        return listFilesInDir(dir);
      })
      .then(entries => {
        if (!entries) return;
        // Filter to YAML files only (not dirs, not pipeline specs)
        const yamlFiles = entries.filter(
          e => !e.isDir && /\.(yaml|yml)$/i.test(e.name)
        );
        setBlueprints(yamlFiles);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const filtered = search
    ? blueprints.filter(b => b.name.toLowerCase().includes(search.toLowerCase()))
    : blueprints;

  const handleDragStart = (e: DragEvent, entry: FileEntry) => {
    const data: PaletteDragData = {
      type: 'stageNode',
      blueprintPath: entry.path,
      label: shortName(entry.name),
    };
    e.dataTransfer.setData('application/reactflow', JSON.stringify(data));
    e.dataTransfer.effectAllowed = 'move';
  };

  return (
    <div className="node-palette">
      <div className="node-palette-header">
        <span className="stage-panel-title">Palette</span>
      </div>

      {/* Add blank stage */}
      <div style={{ padding: '0.5rem' }}>
        <button
          className="hud-button"
          onClick={onAddBlankNode}
          style={{
            width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center',
            gap: '0.3rem', fontSize: '0.72rem',
          }}
        >
          <Plus size={12} /> Add Stage
        </button>
      </div>

      {/* Search */}
      <div style={{ padding: '0 0.5rem 0.4rem' }}>
        <div style={{ position: 'relative' }}>
          <Search
            size={11}
            style={{
              position: 'absolute', left: '0.45rem', top: '50%', transform: 'translateY(-50%)',
              color: 'rgb(var(--muted))', pointerEvents: 'none',
            }}
          />
          <input
            className="hud-input"
            value={search}
            onChange={e => setSearch(e.target.value)}
            placeholder="Search blueprints..."
            style={{
              width: '100%', boxSizing: 'border-box', fontSize: '0.7rem',
              padding: '0.25rem 0.4rem 0.25rem 1.4rem',
            }}
          />
        </div>
      </div>

      {/* Blueprint list */}
      <div className="node-palette-list">
        {loading && (
          <div style={{ fontSize: '0.72rem', color: 'rgb(var(--muted))', padding: '0.5rem', textAlign: 'center' }}>
            Loading...
          </div>
        )}
        {!loading && filtered.length === 0 && (
          <div style={{ fontSize: '0.72rem', color: 'rgb(var(--muted))', padding: '0.5rem', textAlign: 'center' }}>
            {blueprints.length === 0 ? 'No blueprints found' : 'No matches'}
          </div>
        )}
        {filtered.map(entry => (
          <div
            key={entry.path}
            className="node-palette-item"
            draggable
            onDragStart={e => handleDragStart(e, entry)}
            title={`Drag to canvas: ${entry.path}`}
          >
            <FileCode size={12} style={{ color: 'rgb(var(--accent))', flexShrink: 0 }} />
            <span className="node-palette-item-name">
              {shortName(entry.name)}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
