import { useState, useEffect } from 'react';
import { Search, FileCode, Plus } from 'lucide-react';
import { getBlueprintDir, listFilesInDir } from '@/api/client';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';

interface FlowBuilderLandingProps {
  onOpen: (path: string) => void;
}

export function FlowBuilderLanding({ onOpen }: FlowBuilderLandingProps) {
  const [files, setFiles] = useState<{ name: string; path: string }[]>([]);
  const [search, setSearch] = useState('');
  const [loadingFiles, setLoadingFiles] = useState(true);

  // Recursively scan for YAML files in blueprint dir
  useEffect(() => {
    setLoadingFiles(true);
    getBlueprintDir()
      .then(async (rootDir) => {
        if (!rootDir) return;
        const all: { name: string; path: string }[] = [];

        // Scan pipelines/ subdirectory (all YAML files are pipelines there)
        // Plus scan other dirs for files containing "pipeline" in name
        const queue: { dir: string; depth: number; isPipelineDir: boolean }[] = [
          { dir: rootDir, depth: 0, isPipelineDir: false },
        ];
        while (queue.length > 0) {
          const { dir, depth, isPipelineDir } = queue.shift()!;
          try {
            const entries = await listFilesInDir(dir);
            if (!entries) continue;
            for (const e of entries) {
              if (e.isDir && depth < 3) {
                // Mark pipelines/ subtree so all YAML files within are included
                const inPipelines = isPipelineDir || /pipelines?$/i.test(e.name);
                queue.push({ dir: e.path, depth: depth + 1, isPipelineDir: inPipelines });
              } else if (!e.isDir && /\.(yaml|yml)$/i.test(e.name)) {
                if (!isPipelineDir && !e.name.toLowerCase().includes('pipeline')) continue;
                const rel = e.path.startsWith(rootDir)
                  ? e.path.slice(rootDir.length + 1).replace(/\.(yaml|yml)$/i, '')
                  : e.name.replace(/\.(yaml|yml)$/i, '');
                all.push({ name: rel, path: e.path });
              }
            }
          } catch { /* skip inaccessible dirs */ }
        }
        setFiles(all);
      })
      .catch(() => {})
      .finally(() => setLoadingFiles(false));
  }, []);

  const filtered = search
    ? files.filter(f => f.name.toLowerCase().includes(search.toLowerCase()))
    : files;

  const handleNew = () => {
    // Open with a blank canvas — use a temp name, user will Save As
    onOpen('__new__');
  };

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-between mb-6 gap-2 shrink-0">
        <span className="text-xl font-semibold text-foreground tracking-tight">Flow Builder</span>
      </div>

      <div className="flex-1 flex items-center justify-center">
        <div className="bg-card border border-border rounded-lg p-6 w-[380px]">
          <div className="text-xs uppercase tracking-[0.12em] text-muted-foreground mb-3">
            Open Pipeline
          </div>

          {/* Search / combobox */}
          <div className="relative mb-2">
            <Search
              size={12}
              className="absolute left-2 top-1/2 -translate-y-1/2 text-muted-foreground pointer-events-none"
            />
            <Input
              value={search}
              onChange={e => setSearch(e.target.value)}
              placeholder="Search pipelines..."
              autoFocus
              className="w-full text-sm pl-6 pr-2.5 py-1.5"
            />
          </div>

          {/* File list */}
          <div className="max-h-[220px] overflow-auto mb-3 border border-border rounded-sm">
            {loadingFiles ? (
              <div className="p-3 text-center text-muted-foreground text-sm">Loading...</div>
            ) : filtered.length === 0 ? (
              <div className="p-3 text-center text-muted-foreground text-sm">
                {files.length === 0 ? 'No pipeline files found' : 'No matches'}
              </div>
            ) : (
              filtered.map(f => (
                <button
                  key={f.path}
                  onClick={() => onOpen(f.path)}
                  className="flex items-center gap-1.5 w-full px-2.5 py-1.5 bg-transparent border-none border-b border-border/50 text-foreground cursor-pointer font-mono text-sm text-left hover:bg-muted/50 transition-colors"
                >
                  <FileCode size={13} className="text-blue-400 shrink-0" />
                  {f.name}
                </button>
              ))
            )}
          </div>

          {/* Actions */}
          <div className="flex gap-1.5">
            <Button
              onClick={handleNew}
              className="flex-1 justify-center"
            >
              <Plus size={12} /> New Pipeline
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
