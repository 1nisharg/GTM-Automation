"use client";
import { useState, useRef, useCallback, useEffect } from "react";
import {
  Search, Zap, CheckCircle2, Loader2, AlertCircle,
  ChevronRight, Phone, Mail, Linkedin, Globe, Users,
  RefreshCw, XCircle,
} from "lucide-react";
import { toast } from "sonner";

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8000";

// ─────────────────────────────────────────────────────────────────
// TYPES
// ─────────────────────────────────────────────────────────────────

type StageStatus = "idle" | "running" | "done" | "error";

interface Stage {
  id: "discovery" | "enrichment" | "outreach" | "complete";
  label: string;
  description: string;
  icon: string;
}

interface StageState {
  status: StageStatus;
  count?: number;
  data?: any;
  message?: string;
}

interface Partner {
  partner_name: string;
  category: string;
  subcategories: string;
  subcategory_tags: string[] | null;
  website: string;
  region: string;
  phone_number: string | null;
  email_id: string | null;
  linkedin_profile: string | null;
  contact_name: string | null;
  contact_headline: string | null;
}

// ─────────────────────────────────────────────────────────────────
// CONSTANTS
// ─────────────────────────────────────────────────────────────────

const STAGES: Stage[] = [
  { id: "discovery",  label: "Discovery",  description: "Searching partner database",  icon: "🔍" },
  { id: "enrichment", label: "Enrichment", description: "Filling contact details via AI", icon: "🧠" },
  { id: "outreach",   label: "Outreach",   description: "Executing outreach sequence",  icon: "📨" },
  { id: "complete",   label: "Complete",   description: "Pipeline finished",            icon: "✅" },
];

// ─────────────────────────────────────────────────────────────────
// MAIN PAGE
// ─────────────────────────────────────────────────────────────────

export default function PipelineRunnerPage() {
  const [category, setCategory]       = useState("");
  const [categories, setCategories]   = useState<string[]>([]);
  const [showSuggest, setShowSuggest] = useState(false);
  const [running, setRunning]         = useState(false);
  const [searchMode, setSearchMode]   = useState<"subcategory" | "name">("subcategory");
  const [stages, setStages]           = useState<Record<string, StageState>>({
    discovery: { status: "idle" },
    enrichment: { status: "idle" },
    outreach:   { status: "idle" },
    complete:   { status: "idle" },
  });
  const [discoveredPartners, setDiscoveredPartners] = useState<Partner[]>([]);
  const [enrichedPartners,   setEnrichedPartners]   = useState<Partner[]>([]);
  const [activeTab,          setActiveTab]          = useState<"discovered" | "enriched">("discovered");

  const abortRef = useRef<AbortController | null>(null);

  // Fetch category suggestions on mount
  useEffect(() => {
    fetch(`${API_BASE}/api/pipeline/categories`)
      .then(r => r.json())
      .then(d => setCategories(d.categories || []))
      .catch(() => {});
  }, []);

  const filteredSuggestions = category.length > 0
    ? categories
        .filter(c => c.toLowerCase().includes(category.toLowerCase()))
        .sort((a, b) => {
          // Exact match first, then starts-with, then contains
          const al = a.toLowerCase(), bl = b.toLowerCase(), q = category.toLowerCase();
          if (al === q) return -1;
          if (bl === q) return 1;
          if (al.startsWith(q) && !bl.startsWith(q)) return -1;
          if (bl.startsWith(q) && !al.startsWith(q)) return 1;
          return a.localeCompare(b);
        })
        .slice(0, 10)
    : categories.slice(0, 10);

  // ── Update a single stage ──────────────────────────────────────
  const updateStage = useCallback((id: string, patch: Partial<StageState>) => {
    setStages(prev => ({ ...prev, [id]: { ...prev[id], ...patch } }));
  }, []);

  // ── Reset all state ────────────────────────────────────────────
  const resetPipeline = useCallback(() => {
    setStages({
      discovery: { status: "idle" },
      enrichment: { status: "idle" },
      outreach:   { status: "idle" },
      complete:   { status: "idle" },
    });
    setDiscoveredPartners([]);
    setEnrichedPartners([]);
    setActiveTab("discovered");
  }, []);

  // ── Run pipeline via SSE ───────────────────────────────────────
  const handleRun = useCallback(async () => {
    if (!category.trim()) {
      toast.error("Please enter a category first");
      return;
    }
    if (running) return;

    resetPipeline();
    setRunning(true);
    setShowSuggest(false);

    abortRef.current = new AbortController();

    try {
      const payload = searchMode === "subcategory"
        ? { input_category: category.trim() }
        : { input_partner_name: category.trim() };
      const res = await fetch(`${API_BASE}/api/pipeline/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: abortRef.current.signal,
      });

      if (!res.ok || !res.body) {
        throw new Error(`HTTP ${res.status}`);
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n\n");
        buffer = lines.pop() ?? "";

        for (const chunk of lines) {
          const dataLine = chunk.split("\n").find(l => l.startsWith("data: "));
          if (!dataLine) continue;

          try {
            const event = JSON.parse(dataLine.slice(6));
            const { stage, status, data, message } = event;

            if (stage === "error") {
              toast.error(message || "Pipeline error");
              setRunning(false);
              updateStage(stage, { status: "error", message });
              return;
            }

            updateStage(stage, { status, data, count: data?.count });

            if (stage === "discovery" && status === "done" && data?.partners) {
              setDiscoveredPartners(data.partners);
              toast.success(`Discovery: ${data.count} partners found`);
            }
            if (stage === "enrichment" && status === "done" && data?.partners) {
              setEnrichedPartners(data.partners);
              setActiveTab("enriched");
              toast.success(`Enrichment: ${data.count} partners enriched`);
            }
            if (stage === "outreach" && status === "done") {
              toast.info(`Outreach: stage complete`);
            }
            if (stage === "complete" && status === "done") {
              toast.success("🎉 Pipeline complete!", { duration: 4000 });
            }
          } catch { /* skip malformed */ }
        }
      }
    } catch (err: any) {
      if (err.name !== "AbortError") {
        toast.error("Failed to connect to backend. Is it running?");
      }
    } finally {
      setRunning(false);
    }
  }, [category, running, resetPipeline, updateStage]);

  const handleStop = useCallback(() => {
    abortRef.current?.abort();
    setRunning(false);
    toast.info("Pipeline stopped");
  }, []);

  // ─────────────────────────────────────────────────────────────
  return (
    <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500 max-w-6xl mx-auto">

      {/* ── Header ──────────────────────────────────────────────── */}
      <div>
        <div className="flex items-center gap-3 mb-1">
          <h1 className="text-3xl font-bold tracking-tight">Pipeline Command Center</h1>
          {running && (
            <span className="flex items-center gap-1.5 bg-primary/15 text-primary text-[11px] font-bold px-2.5 py-1 rounded-full">
              <Loader2 size={11} className="animate-spin" />
              RUNNING
            </span>
          )}
        </div>
        <p className="text-sm text-muted-foreground">
          Discovery → Enrichment → Outreach · Live agentic pipeline with streaming results
        </p>
      </div>

      {/* ── Input + Run ─────────────────────────────────────────── */}
      <div className="bg-card border border-border rounded-2xl p-6 shadow-sm">

        {/* Mode toggle */}
        <div className="flex items-center gap-2 mb-4">
          <span className="text-xs font-bold text-muted-foreground uppercase tracking-widest">
            Search by
          </span>
          <div className="flex rounded-lg border border-border overflow-hidden text-xs font-semibold">
            <button
              onClick={() => { setSearchMode("subcategory"); setCategory(""); }}
              disabled={running}
              className={`px-4 py-1.5 transition-colors ${
                searchMode === "subcategory"
                  ? "bg-primary text-primary-foreground"
                  : "bg-background text-muted-foreground hover:text-foreground hover:bg-muted"
              }`}
            >
              Subcategory
            </button>
            <button
              onClick={() => { setSearchMode("name"); setCategory(""); }}
              disabled={running}
              className={`px-4 py-1.5 transition-colors border-l border-border ${
                searchMode === "name"
                  ? "bg-primary text-primary-foreground"
                  : "bg-background text-muted-foreground hover:text-foreground hover:bg-muted"
              }`}
            >
              Partner Name
            </button>
          </div>
        </div>

        <div className="flex gap-3">
          <div className="relative flex-1">
            <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground pointer-events-none" />
            <input
              id="category-input"
              name="category-input"
              type="text"
              value={category}
              onChange={e => { setCategory(e.target.value); setShowSuggest(searchMode === "subcategory"); }}
              onFocus={() => { if (searchMode === "subcategory") setShowSuggest(true); }}
              onBlur={() => setTimeout(() => setShowSuggest(false), 150)}
              onKeyDown={e => e.key === "Enter" && handleRun()}
              placeholder={
                searchMode === "subcategory"
                  ? 'e.g. "Adventure & Extreme Sports"'
                  : 'e.g. "Adventure HQ"'
              }
              className="w-full bg-background border border-border rounded-xl pl-9 pr-4 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-primary/50 placeholder:text-muted-foreground/50 transition-all"
              disabled={running}
              autoComplete="off"
              data-1p-ignore
              data-lpignore="true"
              data-bwignore="true"
            />
            {/* Autocomplete — only for subcategory mode */}
            {searchMode === "subcategory" && showSuggest && filteredSuggestions.length > 0 && (
              <div className="absolute top-full left-0 right-0 mt-1 z-50 bg-card border border-border rounded-xl shadow-2xl overflow-hidden">
                {filteredSuggestions.map(s => (
                  <button
                    key={s}
                    onMouseDown={() => { setCategory(s); setShowSuggest(false); }}
                    title={s}
                    className="w-full text-left px-4 py-2.5 text-sm hover:bg-primary/10 hover:text-primary transition-colors"
                  >
                    {s}
                  </button>
                ))}
              </div>
            )}
          </div>

          {running ? (
            <button
              onClick={handleStop}
              className="px-5 py-3 rounded-xl bg-destructive/10 text-destructive border border-destructive/30 font-bold text-sm flex items-center gap-2 hover:bg-destructive/20 transition-colors"
            >
              <XCircle size={16} /> Stop
            </button>
          ) : (
            <button
              id="run-pipeline-btn"
              onClick={handleRun}
              className="px-6 py-3 rounded-xl bg-primary text-primary-foreground font-bold text-sm flex items-center gap-2 hover:bg-primary/90 transition-all active:scale-[0.98] shadow-lg shadow-primary/20"
            >
              <Zap size={16} fill="currentColor" /> Run Pipeline
            </button>
          )}

          {!running && (Object.values(stages).some(s => s.status !== "idle")) && (
            <button
              onClick={resetPipeline}
              title="Reset"
              className="px-3 py-3 rounded-xl border border-border hover:bg-muted transition-colors"
            >
              <RefreshCw size={16} />
            </button>
          )}
        </div>
      </div>

      {/* ── Stage Progress Rail ──────────────────────────────────── */}
      <div className="grid grid-cols-4 gap-3">
        {STAGES.map((stage, i) => {
          const s = stages[stage.id];
          return (
            <StageCard
              key={stage.id}
              stage={stage}
              state={s}
              isLast={i === STAGES.length - 1}
            />
          );
        })}
      </div>

      {/* ── Results Tabs ─────────────────────────────────────────── */}
      {(discoveredPartners.length > 0 || enrichedPartners.length > 0) && (
        <div className="bg-card border border-border rounded-2xl shadow-sm overflow-hidden animate-in fade-in duration-300">
          <div className="flex border-b border-border">
            <TabBtn
              active={activeTab === "discovered"}
              onClick={() => setActiveTab("discovered")}
              label={`Discovered (${discoveredPartners.length})`}
            />
            <TabBtn
              active={activeTab === "enriched"}
              onClick={() => setActiveTab("enriched")}
              label={`Enriched (${enrichedPartners.length})`}
            />
          </div>

          <div className="overflow-x-auto">
            {activeTab === "discovered" && (
              <PartnerTable partners={discoveredPartners} mode="discovery" />
            )}
            {activeTab === "enriched" && (
              <PartnerTable partners={enrichedPartners} mode="enriched" />
            )}
          </div>
        </div>
      )}

      {/* ── Empty state ──────────────────────────────────────────── */}
      {!running && discoveredPartners.length === 0 && (
        <div className="flex flex-col items-center justify-center py-16 text-muted-foreground gap-4">
          <div className="w-16 h-16 rounded-2xl bg-primary/10 flex items-center justify-center">
            <Users size={28} className="text-primary" />
          </div>
          <div className="text-center">
            <p className="font-medium">No pipeline run yet</p>
            <p className="text-sm mt-1">Enter a category above and click <strong>Run Pipeline</strong> to start.</p>
          </div>
        </div>
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────
// STAGE CARD
// ─────────────────────────────────────────────────────────────────

function StageCard({ stage, state, isLast }: { stage: Stage; state: StageState; isLast: boolean }) {
  const isIdle    = state.status === "idle";
  const isRunning = state.status === "running";
  const isDone    = state.status === "done";
  const isError   = state.status === "error";

  return (
    <div className={`
      relative bg-card border rounded-xl p-4 flex flex-col gap-2 transition-all duration-300
      ${isRunning ? "border-primary/60 shadow-lg shadow-primary/10 animate-pulse-border" : ""}
      ${isDone    ? "border-success/40 bg-success/5" : ""}
      ${isError   ? "border-destructive/40 bg-destructive/5" : ""}
      ${isIdle    ? "border-border opacity-60" : ""}
      stage-enter
    `}>
      <div className="flex items-center justify-between">
        <span className="text-xl">{stage.icon}</span>
        {isRunning && <Loader2 size={14} className="animate-spin text-primary" />}
        {isDone    && <CheckCircle2 size={14} className="text-success" />}
        {isError   && <AlertCircle  size={14} className="text-destructive" />}
      </div>
      <div>
        <div className="font-bold text-sm">{stage.label}</div>
        <div className="text-[11px] text-muted-foreground mt-0.5">
          {isRunning ? (
            <span className="text-primary font-medium">{stage.description}…</span>
          ) : isDone && state.count !== undefined ? (
            <span className="text-success font-medium">{state.count} {stage.id === "complete" ? "done" : "partners"}</span>
          ) : isError ? (
            <span className="text-destructive">{state.message || "Error"}</span>
          ) : (
            stage.description
          )}
        </div>
      </div>

      {/* Connector arrow */}
      {!isLast && (
        <ChevronRight
          size={14}
          className={`absolute -right-2.5 top-1/2 -translate-y-1/2 z-10
            ${isDone ? "text-success" : "text-muted-foreground/30"}`}
        />
      )}
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────
// PARTNER TABLE
// ─────────────────────────────────────────────────────────────────

function PartnerTable({ partners, mode }: { partners: Partner[]; mode: "discovery" | "enriched" }) {
  if (partners.length === 0) {
    return (
      <div className="py-12 text-center text-muted-foreground text-sm">
        No partners to display.
      </div>
    );
  }

  return (
    <table className="w-full text-sm">
      <thead className="text-xs text-muted-foreground uppercase bg-background/40 border-b border-border">
        <tr>
          <th className="px-4 py-3 text-left font-medium">Partner</th>
          <th className="px-4 py-3 text-left font-medium">Category</th>
          <th className="px-4 py-3 text-left font-medium">Region</th>
          {mode === "enriched" && (
            <>
              <th className="px-4 py-3 text-left font-medium">Contact</th>
              <th className="px-4 py-3 text-left font-medium">Channels</th>
            </>
          )}
          <th className="px-4 py-3 text-left font-medium">Website</th>
        </tr>
      </thead>
      <tbody className="divide-y divide-border">
        {partners.map((p, i) => (
          <tr key={i} className="hover:bg-muted/30 transition-colors group animate-in fade-in duration-200">
            <td className="px-4 py-3 font-medium">
              {p.partner_name || "—"}
            </td>
            <td className="px-4 py-3">
              <span className="bg-secondary text-secondary-foreground text-[10px] px-1.5 py-0.5 rounded uppercase">
                {p.category || "—"}
              </span>
            </td>
            <td className="px-4 py-3 text-muted-foreground text-xs">{p.region || "—"}</td>

            {mode === "enriched" && (
              <>
                <td className="px-4 py-3 text-xs text-muted-foreground">
                  {p.contact_name ? (
                    <div>
                      <div className="font-medium text-foreground">{p.contact_name}</div>
                      <div className="text-[11px]">{p.contact_headline}</div>
                    </div>
                  ) : "—"}
                </td>
                <td className="px-4 py-3">
                  <div className="flex gap-2">
                    {p.phone_number && (
                      <span title={p.phone_number} className="text-emerald-400">
                        <Phone size={13} />
                      </span>
                    )}
                    {p.email_id && (
                      <span title={p.email_id} className="text-amber-400">
                        <Mail size={13} />
                      </span>
                    )}
                    {p.linkedin_profile && (
                      <a
                        href={p.linkedin_profile}
                        target="_blank"
                        rel="noopener noreferrer"
                        title={p.linkedin_profile}
                        className="text-blue-400 hover:text-blue-300"
                      >
                        <Linkedin size={13} />
                      </a>
                    )}
                    {!p.phone_number && !p.email_id && !p.linkedin_profile && (
                      <span className="text-muted-foreground text-[11px]">None found</span>
                    )}
                  </div>
                </td>
              </>
            )}

            <td className="px-4 py-3">
              {p.website ? (
                <a
                  href={p.website.startsWith("http") ? p.website : `https://${p.website}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center gap-1 text-primary text-xs hover:underline"
                >
                  <Globe size={11} /> {p.website.replace(/^https?:\/\//, "").split("/")[0]}
                </a>
              ) : "—"}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

// ─────────────────────────────────────────────────────────────────
// TAB BUTTON
// ─────────────────────────────────────────────────────────────────

function TabBtn({ active, onClick, label }: { active: boolean; onClick: () => void; label: string }) {
  return (
    <button
      onClick={onClick}
      className={`px-5 py-3 text-sm font-medium border-b-2 transition-colors ${
        active
          ? "border-primary text-primary"
          : "border-transparent text-muted-foreground hover:text-foreground"
      }`}
    >
      {label}
    </button>
  );
}