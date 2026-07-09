"use client";
import { useState, useEffect, useRef } from "react";
import {
  Send, MessageSquare, Mail, Link2, Zap, CheckCircle2,
  Clock, XCircle, Loader2, X, RefreshCw, Phone,
  TrendingUp, TrendingDown, Minus, FileText, ChevronDown, ChevronUp,
} from "lucide-react";
import { FaInstagram } from "react-icons/fa";
import { toast } from "sonner";

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8000";

// ─────────────────────────────────────────────────────────────────
// TYPES
// ─────────────────────────────────────────────────────────────────

interface OutreachLead {
  id: string;
  business_name: string;
  category: string;
  company_synopsis: string | null;
  score: number;
  score_tier: string;
  phone: string | null;
  email: string | null;
  linkedin_url: string | null;
  instagram: string | null;
  attempts: number;
  last_channel: string;
  last_status: string;
}

interface ChannelStat { channel: string; count: number; }

interface ChannelResult {
  channel: string;
  result: {
    status: string;
    note?: string;
    sid?: string;
    to?: string;
    summary?: {
      outcome?: string;
      sentiment?: string;
      key_points?: string[];
      action_items?: string[];
      notable_quotes?: string[];
    } | null;
    duration_s?: number;
    call_sid?: string;
  };
}

interface OutreachResult {
  lead_name: string;
  results: ChannelResult[];
}

// ─────────────────────────────────────────────────────────────────
// SENTIMENT BADGE
// ─────────────────────────────────────────────────────────────────

function SentimentBadge({ sentiment }: { sentiment: string }) {
  const s = sentiment?.toLowerCase() ?? "";
  if (s.includes("positive"))
    return <span className="flex items-center gap-1 text-emerald-400 text-xs font-bold"><TrendingUp size={12} /> Positive</span>;
  if (s.includes("negative"))
    return <span className="flex items-center gap-1 text-red-400 text-xs font-bold"><TrendingDown size={12} /> Negative</span>;
  return <span className="flex items-center gap-1 text-amber-400 text-xs font-bold"><Minus size={12} /> Neutral</span>;
}

// ─────────────────────────────────────────────────────────────────
// OUTREACH RESULT PANEL
// ─────────────────────────────────────────────────────────────────

function OutreachResultPanel({ result, onClose }: { result: OutreachResult; onClose: () => void }) {
  const [expanded, setExpanded] = useState<string | null>(null);

  const statusColor = (status: string) => {
    const s = status?.toLowerCase() ?? "";
    if (s === "sent" || s === "completed") return "text-emerald-400";
    if (s === "skipped" || s === "not_implemented") return "text-amber-400";
    return "text-red-400";
  };

  const statusIcon = (status: string) => {
    const s = status?.toLowerCase() ?? "";
    if (s === "sent" || s === "completed") return <CheckCircle2 size={14} className="text-emerald-400" />;
    if (s === "skipped" || s === "not_implemented") return <Clock size={14} className="text-amber-400" />;
    return <XCircle size={14} className="text-red-400" />;
  };

  const channelIcon = (ch: string) => {
    const m: Record<string, React.ReactNode> = {
      whatsapp: <MessageSquare size={14} className="text-emerald-400" />,
      email:    <Mail size={14} className="text-amber-400" />,
      linkedin: <Link2 size={14} className="text-blue-400" />,
      voice:    <Phone size={14} className="text-violet-400" />,
    };
    return m[ch] ?? <Send size={14} />;
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm animate-in fade-in duration-150">
      <div className="bg-card border border-border rounded-2xl shadow-2xl w-full max-w-lg mx-4 max-h-[85vh] overflow-y-auto animate-in zoom-in-95 duration-200">
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-border sticky top-0 bg-card z-10">
          <div>
            <h2 className="text-base font-bold flex items-center gap-2">
              <CheckCircle2 size={16} className="text-emerald-400" /> Outreach Complete
            </h2>
            <p className="text-xs text-muted-foreground mt-0.5">{result.lead_name}</p>
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground p-1 rounded-md hover:bg-muted transition-colors">
            <X size={16} />
          </button>
        </div>

        {/* Channel results */}
        <div className="p-5 space-y-3">
          {result.results.map((r) => {
            const isVoice    = r.channel === "voice";
            const summary    = r.result?.summary;
            const hasDetail  = isVoice && summary;
            const isExpanded = expanded === r.channel;

            return (
              <div key={r.channel} className="border border-border rounded-xl overflow-hidden">
                {/* Channel row */}
                <div className="flex items-center gap-3 p-3">
                  {channelIcon(r.channel)}
                  <span className="text-sm font-medium capitalize flex-1">{r.channel}</span>
                  {statusIcon(r.result.status)}
                  <span className={`text-xs font-bold ${statusColor(r.result.status)}`}>
                    {r.result.status}
                  </span>
                  {hasDetail && (
                    <button
                      onClick={() => setExpanded(isExpanded ? null : r.channel)}
                      className="ml-2 text-muted-foreground hover:text-foreground transition-colors"
                    >
                      {isExpanded ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
                    </button>
                  )}
                </div>

                {/* Voice detail panel */}
                {hasDetail && isExpanded && summary && (
                  <div className="border-t border-border bg-background/50 p-4 space-y-4">

                    {/* Sentiment */}
                    {summary.sentiment && (
                      <div className="flex items-center gap-2">
                        <span className="text-xs text-muted-foreground w-20">Sentiment</span>
                        <SentimentBadge sentiment={summary.sentiment} />
                        <span className="text-xs text-muted-foreground">
                          — {summary.sentiment.split("—").slice(1).join("—").trim() || summary.sentiment}
                        </span>
                      </div>
                    )}

                    {/* Outcome */}
                    {summary.outcome && (
                      <div>
                        <span className="text-xs text-muted-foreground uppercase tracking-wider block mb-1">Outcome</span>
                        <p className="text-sm text-foreground leading-relaxed">{summary.outcome}</p>
                      </div>
                    )}

                    {/* Key points */}
                    {summary.key_points && summary.key_points.length > 0 && (
                      <div>
                        <span className="text-xs text-muted-foreground uppercase tracking-wider block mb-1">Key Points</span>
                        <ul className="space-y-1">
                          {summary.key_points.map((p, i) => (
                            <li key={i} className="text-xs flex gap-2">
                              <span className="text-primary mt-0.5">•</span>
                              <span>{p}</span>
                            </li>
                          ))}
                        </ul>
                      </div>
                    )}

                    {/* Action items */}
                    {summary.action_items && summary.action_items.length > 0 && (
                      <div>
                        <span className="text-xs text-muted-foreground uppercase tracking-wider block mb-1">Action Items</span>
                        <ul className="space-y-1">
                          {summary.action_items.map((a, i) => (
                            <li key={i} className="text-xs flex gap-2">
                              <span className="text-amber-400 mt-0.5">→</span>
                              <span>{a}</span>
                            </li>
                          ))}
                        </ul>
                      </div>
                    )}

                    {/* Notable quotes */}
                    {summary.notable_quotes && summary.notable_quotes.length > 0 && (
                      <div>
                        <span className="text-xs text-muted-foreground uppercase tracking-wider block mb-1">Notable Quotes</span>
                        {summary.notable_quotes.map((q, i) => (
                          <p key={i} className="text-xs italic text-muted-foreground border-l-2 border-primary/40 pl-2 mb-1">"{q}"</p>
                        ))}
                      </div>
                    )}

                    {/* Duration */}
                    {r.result.duration_s !== undefined && r.result.duration_s > 0 && (
                      <div className="flex items-center gap-2 text-xs text-muted-foreground">
                        <Phone size={11} />
                        <span>Call duration: {r.result.duration_s}s</span>
                      </div>
                    )}
                  </div>
                )}

                {/* No-call note */}
                {isVoice && r.result.status === "skipped" && (
                  <div className="border-t border-border bg-background/50 px-4 py-2 text-xs text-muted-foreground">
                    {r.result.note || "No phone number — voice call skipped"}
                  </div>
                )}

                {/* Non-voice note */}
                {!isVoice && r.result.note && (
                  <div className="border-t border-border bg-background/50 px-4 py-2 text-xs text-muted-foreground">
                    {r.result.note}
                  </div>
                )}
              </div>
            );
          })}
        </div>

        <div className="px-5 pb-5">
          <button
            onClick={onClose}
            className="w-full py-2 rounded-lg border border-border text-sm hover:bg-muted transition-colors"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────
// CHANNEL CONFIG
// ─────────────────────────────────────────────────────────────────

const CHANNELS = [
  { id: "whatsapp", label: "WhatsApp",    description: "Send via Twilio",         icon: MessageSquare, color: "text-emerald-400", bg: "bg-emerald-500/10 border-emerald-500/30", activeBg: "bg-emerald-500/20 border-emerald-400", field: "phone" },
  { id: "linkedin", label: "LinkedIn DM", description: "Send via Unipile",        icon: Link2,         color: "text-blue-400",   bg: "bg-blue-500/10 border-blue-500/30",       activeBg: "bg-blue-500/20 border-blue-400",       field: "linkedin_url" },
  { id: "email",    label: "Email",       description: "Send via Gmail SMTP",      icon: Mail,          color: "text-amber-400",  bg: "bg-amber-500/10 border-amber-500/30",     activeBg: "bg-amber-500/20 border-amber-400",     field: "email" },
  { id: "instagram",label: "Instagram",   description: "Auto-discover via Tavily", icon: FaInstagram,  color: "text-pink-400",   bg: "bg-pink-500/10 border-pink-500/30",       activeBg: "bg-pink-500/20 border-pink-400",       field: "instagram" },
] as const;

// ─────────────────────────────────────────────────────────────────
// CHANNEL MODAL
// ─────────────────────────────────────────────────────────────────

function ChannelModal({ lead, onClose, onLaunch, launching }: {
  lead: OutreachLead | null;
  onClose: () => void;
  onLaunch: (partnerName: string | null, channels: string[], msg: string) => void;
  launching: boolean;
}) {
  const [selected, setSelected] = useState<string[]>(["whatsapp"]);
  const [message,  setMessage]  = useState("");
  const backdropRef = useRef<HTMLDivElement>(null);

  const toggle = (id: string) =>
    setSelected(prev => prev.includes(id) ? prev.filter(c => c !== id) : [...prev, id]);

  const isAvailable = (ch: typeof CHANNELS[number]) => {
    if (!lead) return true;
    if (ch.id === "instagram") return true;
    return !!(lead as any)[ch.field];
  };

  return (
    <div
      ref={backdropRef}
      onClick={e => e.target === backdropRef.current && onClose()}
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm animate-in fade-in duration-150"
    >
      <div className="bg-card border border-border rounded-2xl shadow-2xl w-full max-w-md mx-4 animate-in zoom-in-95 duration-200">
        <div className="flex items-center justify-between p-5 border-b border-border">
          <div>
            <h2 className="text-base font-bold flex items-center gap-2">
              <Zap size={16} className="text-primary" /> Launch Outreach Sequence
            </h2>
            {lead && (
              <p className="text-xs text-muted-foreground mt-0.5">
                {lead.business_name}
                <span className="ml-2 bg-secondary text-secondary-foreground px-1.5 py-0.5 rounded text-[10px] uppercase">{lead.category}</span>
              </p>
            )}
            {!lead && <p className="text-xs text-muted-foreground mt-0.5">All filtered leads (bulk)</p>}
          </div>
          <button onClick={onClose} className="text-muted-foreground hover:text-foreground p-1 rounded-md hover:bg-muted transition-colors">
            <X size={16} />
          </button>
        </div>

        <div className="p-5 space-y-3">
          <p className="text-xs text-muted-foreground font-medium uppercase tracking-wider mb-3">
            Select channels — messages sent in this order
          </p>
          {CHANNELS.map(ch => {
            const Icon      = ch.icon;
            const active    = selected.includes(ch.id);
            const available = isAvailable(ch);
            const order     = selected.indexOf(ch.id) + 1;
            return (
              <button
                key={ch.id}
                onClick={() => available && toggle(ch.id)}
                disabled={!available}
                className={`w-full flex items-center gap-3 p-3 rounded-xl border transition-all text-left
                  ${active ? ch.activeBg : ch.bg}
                  ${!available ? "opacity-40 cursor-not-allowed" : "cursor-pointer hover:opacity-90"}`}
              >
                <div className={`w-6 h-6 rounded-full flex items-center justify-center text-[10px] font-bold flex-shrink-0 transition-all ${active ? "bg-primary text-primary-foreground" : "bg-muted text-muted-foreground"}`}>
                  {active ? order : <div className="w-2 h-2 rounded-full bg-current opacity-40" />}
                </div>
                <Icon size={16} className={ch.color} />
                <div className="flex-1 min-w-0">
                  <div className="text-sm font-medium">{ch.label}</div>
                  <div className="text-[11px] text-muted-foreground">
                    {available ? ch.description : (ch.id === "instagram" ? ch.description : `${ch.field} — not on record`)}
                  </div>
                </div>
                {active && <CheckCircle2 size={16} className="text-primary flex-shrink-0" />}
              </button>
            );
          })}
        </div>

        <div className="px-5 pb-3">
          <label className="text-xs text-muted-foreground font-medium uppercase tracking-wider block mb-2">
            Custom message <span className="normal-case font-normal">(optional — leave blank to auto-generate)</span>
          </label>
          <textarea
            value={message}
            onChange={e => setMessage(e.target.value)}
            placeholder="Hi [Name], we'd love to explore a partnership…"
            rows={3}
            className="w-full bg-background border border-border rounded-lg px-3 py-2 text-sm resize-none focus:outline-none focus:ring-1 focus:ring-primary placeholder:text-muted-foreground/50"
          />
        </div>

        <div className="flex items-center justify-between p-5 pt-3 border-t border-border">
          <span className="text-xs text-muted-foreground">
            {selected.length === 0 ? "Select at least one channel" : `${selected.length} channel${selected.length > 1 ? "s" : ""} selected`}
          </span>
          <div className="flex gap-2">
            <button onClick={onClose} className="px-4 py-2 rounded-lg text-sm border border-border hover:bg-muted transition-colors">
              Cancel
            </button>
            <button
              onClick={() => onLaunch(lead?.business_name ?? null, selected, message)}
              disabled={selected.length === 0 || launching}
              className="px-4 py-2 rounded-lg text-sm bg-primary text-primary-foreground font-bold hover:bg-primary/90 transition-colors disabled:opacity-50 flex items-center gap-2"
            >
              {launching ? <><Loader2 size={14} className="animate-spin" /> Launching…</> : <><Zap size={14} /> Launch</>}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ─────────────────────────────────────────────────────────────────
// MAIN PAGE
// ─────────────────────────────────────────────────────────────────

export default function OutreachPage() {
  const [leads,         setLeads]         = useState<OutreachLead[]>([]);
  const [channels,      setChannels]      = useState<ChannelStat[]>([]);
  const [loading,       setLoading]       = useState(true);
  const [filter,        setFilter]        = useState("all");
  const [modalOpen,     setModalOpen]     = useState(false);
  const [selectedLead,  setSelectedLead]  = useState<OutreachLead | null>(null);
  const [launching,     setLaunching]     = useState(false);
  const [search,        setSearch]        = useState("");
  const [outreachResult, setOutreachResult] = useState<OutreachResult | null>(null);

  // ── Manual test row ──────────────────────────────────────────────
  const [testPhone,     setTestPhone]     = useState("");
  const [testEmail,     setTestEmail]     = useState("");
  const [testLinkedin,  setTestLinkedin]  = useState("");
  const [testName,      setTestName]      = useState("Test Partner");
  const [testContactName, setTestContactName] = useState("");
  const [testCategory,  setTestCategory]  = useState("");
  const [testSynopsis,  setTestSynopsis]  = useState("");
  const [testLaunching, setTestLaunching] = useState(false);

  const fetchLeads = async () => {
    setLoading(true);
    try {
      const res  = await fetch(`${API_BASE}/api/outreach/`);
      const data = await res.json();
      setLeads(data.leads    || []);
      setChannels(data.channels || []);
    } catch {
      toast.error("Failed to load outreach data");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchLeads(); }, []);

  const openLeadModal = (lead: OutreachLead) => { setSelectedLead(lead); setModalOpen(true); };
  const openBulkModal = () => { setSelectedLead(null); setModalOpen(true); };

  const handleLaunch = async (partnerName: string | null, selectedChannels: string[], customMsg: string) => {
    setLaunching(true);
    const tid = "outreach-launch";
    toast.loading("Launching outreach sequence…", { id: tid });
    try {
      if (partnerName) {
        const res  = await fetch(`${API_BASE}/api/outreach/launch`, {
          method:  "POST",
          headers: { "Content-Type": "application/json" },
          body:    JSON.stringify({ partner_name: partnerName, channels: selectedChannels, custom_message: customMsg }),
        });
        const data = await res.json();
        toast.dismiss(tid);
        toast.success(`Outreach launched for ${partnerName}`);
        setModalOpen(false);
        // Show result panel with sentiment
        setOutreachResult(data as OutreachResult);
      } else {
        for (const lead of filteredLeads) {
          await fetch(`${API_BASE}/api/outreach/launch`, {
            method:  "POST",
            headers: { "Content-Type": "application/json" },
            body:    JSON.stringify({ partner_name: lead.business_name, channels: selectedChannels, custom_message: customMsg }),
          });
        }
        toast.dismiss(tid);
        toast.success(`Bulk outreach launched for ${filteredLeads.length} leads`);
        setModalOpen(false);
      }
      fetchLeads();
    } catch {
      toast.error("Outreach launch failed", { id: tid });
    } finally {
      setLaunching(false);
    }
  };

  const handleTestLaunch = async () => {
    if (!testPhone && !testEmail && !testLinkedin) {
      toast.error("Enter at least one contact field to test");
      return;
    }
    setTestLaunching(true);
    const tid = "test-launch";
    toast.loading("Launching test outreach…", { id: tid });
    const chans: string[] = [];
    if (testPhone)    chans.push("whatsapp");
    if (testPhone)    chans.push("voice");
    if (testEmail)    chans.push("email");
    if (testLinkedin) chans.push("linkedin");
    try {
      const res  = await fetch(`${API_BASE}/api/outreach/launch`, {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify({
          partner_name:   testName,
          channels:       chans,
          custom_message: "",
          test_override: {
            phone_number:     testPhone,
            email_id:         testEmail,
            linkedin_profile: testLinkedin,
            contact_name:     testContactName,
            category:         testCategory,
            company_synopsis: testSynopsis,
          },
        }),
      });
      const data = await res.json();
      toast.dismiss(tid);
      toast.success("Test outreach sent");
      // Show result panel with sentiment
      setOutreachResult(data as OutreachResult);
    } catch {
      toast.error("Test launch failed", { id: tid });
    } finally {
      setTestLaunching(false);
    }
  };

  const channelIcon = (ch: string) => {
    const m: Record<string, React.ReactNode> = {
      whatsapp:  <MessageSquare size={14} className="text-emerald-400" />,
      email:     <Mail size={14} className="text-amber-400" />,
      linkedin:  <Link2 size={14} className="text-blue-400" />,
      instagram: <FaInstagram size={14} className="text-pink-400" />,
      voice:     <Phone size={14} className="text-violet-400" />,
    };
    return m[ch?.toLowerCase()] || <Send size={14} />;
  };

  const statusIcon = (s: string) => {
    const sl = s?.toLowerCase() ?? "";
    if (sl.includes("sent") || sl.includes("responded") || sl.includes("delivered"))
      return <CheckCircle2 size={12} className="text-emerald-400" />;
    if (sl.includes("pending") || sl.includes("skipped"))
      return <Clock size={12} className="text-amber-400" />;
    return <XCircle size={12} className="text-destructive" />;
  };

  const filteredLeads = leads.filter(l => {
    if (filter === "responded" && !l.last_status.toLowerCase().includes("responded")) return false;
    if (filter === "no_reply"  &&  l.last_status.toLowerCase().includes("responded")) return false;
    if (search) {
      const t = search.toLowerCase();
      return l.business_name?.toLowerCase().includes(t) || l.category?.toLowerCase().includes(t);
    }
    return true;
  });

  return (
    <>
      {/* Result panel with sentiment */}
      {outreachResult && (
        <OutreachResultPanel
          result={outreachResult}
          onClose={() => setOutreachResult(null)}
        />
      )}

      {modalOpen && (
        <ChannelModal
          lead={selectedLead}
          onClose={() => setModalOpen(false)}
          onLaunch={handleLaunch}
          launching={launching}
        />
      )}

      <div className="space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-200">
        {/* Header */}
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-3xl font-bold flex items-center gap-3">
              <Send className="text-primary" /> Outreach Command
            </h1>
            <p className="text-sm text-muted-foreground mt-1">
              Multi-channel outreach: WhatsApp → LinkedIn → Email → Instagram
            </p>
          </div>
          <div className="flex gap-2">
            <button onClick={fetchLeads} className="border border-border bg-card hover:bg-muted p-2 rounded-lg transition-all">
              <RefreshCw size={14} />
            </button>
            <button
              onClick={openBulkModal}
              className="bg-primary hover:bg-primary/90 text-primary-foreground px-4 py-2 rounded-md font-bold text-sm flex items-center gap-2 transition-colors"
            >
              <Zap size={16} /> Launch Sequence
            </button>
          </div>
        </div>

        {/* Channel stats */}
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
          {["whatsapp", "email", "linkedin", "voice", "instagram"].map(ch => {
            const stat = channels.find(c => c.channel === ch);
            return (
              <div key={ch} className="bg-card border border-border rounded-xl p-4 shadow-sm flex items-center gap-3">
                {channelIcon(ch)}
                <div>
                  <div className="text-xs text-muted-foreground capitalize">{ch}</div>
                  <div className="text-lg font-bold">{stat?.count || 0}</div>
                </div>
              </div>
            );
          })}
        </div>

        {/* Table */}
        <div className="bg-card border border-border rounded-xl shadow-sm">
          <div className="p-4 border-b border-border flex justify-between items-center">
            <div className="flex items-center gap-3">
              <span className="font-bold">Outreach Ready ({filteredLeads.length})</span>
              <input
                value={search}
                onChange={e => setSearch(e.target.value)}
                placeholder="Search…"
                className="bg-background border border-border rounded-lg px-3 py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-primary/50 w-40"
              />
            </div>
            <div className="flex gap-2 text-xs">
              {["all", "responded", "no_reply"].map(f => (
                <button
                  key={f}
                  onClick={() => setFilter(f)}
                  className={`border border-border px-3 py-1.5 rounded transition-colors ${filter === f ? "bg-primary/20 text-primary font-bold" : "hover:bg-muted"}`}
                >
                  {f === "all" ? "All" : f === "responded" ? "Responded" : "No Reply"}
                </button>
              ))}
            </div>
          </div>

          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="text-xs text-muted-foreground uppercase bg-background/30 border-b border-border">
                <tr>
                  <th className="px-4 py-3 text-left font-medium">Business</th>
                  <th className="px-4 py-3 text-left font-medium">Category</th>
                  <th className="px-4 py-3 text-left font-medium">Synopsis</th>
                  <th className="px-4 py-3 text-left font-medium">Channels</th>
                  <th className="px-4 py-3 text-left font-medium">Last Channel</th>
                  <th className="px-4 py-3 text-left font-medium">Status</th>
                  <th className="px-4 py-3 text-right font-medium">Actions</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {/* ── Manual test row ── */}
                <tr className="bg-amber-500/5 border-b-2 border-amber-500/30">
                  <td className="px-4 py-3">
                    <div className="flex flex-col gap-1.5">
                      <input
                        value={testName}
                        onChange={e => setTestName(e.target.value)}
                        placeholder="Test Company Name"
                        className="bg-background border border-amber-500/40 rounded px-2 py-1 text-xs w-40 focus:outline-none focus:ring-1 focus:ring-amber-500"
                      />
                      <input
                        value={testContactName}
                        onChange={e => setTestContactName(e.target.value)}
                        placeholder="Test Contact Name (person)"
                        className="bg-background border border-amber-500/40 rounded px-2 py-1 text-xs w-40 focus:outline-none focus:ring-1 focus:ring-amber-500"
                      />
                    </div>
                  </td>
                  <td className="px-4 py-3">
                    <span className="bg-amber-500/20 text-amber-400 text-[10px] px-1.5 py-0.5 rounded uppercase font-bold">TEST</span>
                  </td>
                  <td className="px-4 py-3">
                    <div className="flex flex-col gap-1.5">
                      <input
                        value={testCategory}
                        onChange={e => setTestCategory(e.target.value)}
                        placeholder="Category (e.g. Restaurant)"
                        className="bg-background border border-amber-500/40 rounded px-2 py-0.5 text-xs w-40 focus:outline-none focus:ring-1 focus:ring-amber-500"
                      />
                      <textarea
                        value={testSynopsis}
                        onChange={e => setTestSynopsis(e.target.value)}
                        placeholder="Synopsis (1-3 sentences)…"
                        rows={2}
                        className="bg-background border border-amber-500/40 rounded px-2 py-0.5 text-xs w-40 resize-none focus:outline-none focus:ring-1 focus:ring-amber-500"
                      />
                    </div>
                  </td>
                  <td className="px-4 py-3">
                    <div className="flex flex-col gap-1.5">
                      <div className="flex items-center gap-1.5">
                        <MessageSquare size={11} className="text-emerald-400 flex-shrink-0" />
                        <input value={testPhone} onChange={e => setTestPhone(e.target.value)} placeholder="+971xxxxxxxxx" className="bg-background border border-border rounded px-2 py-0.5 text-xs w-32 focus:outline-none focus:ring-1 focus:ring-primary/50" />
                      </div>
                      <div className="flex items-center gap-1.5">
                        <Mail size={11} className="text-amber-400 flex-shrink-0" />
                        <input value={testEmail} onChange={e => setTestEmail(e.target.value)} placeholder="you@email.com" className="bg-background border border-border rounded px-2 py-0.5 text-xs w-32 focus:outline-none focus:ring-1 focus:ring-primary/50" />
                      </div>
                      <div className="flex items-center gap-1.5">
                        <Link2 size={11} className="text-blue-400 flex-shrink-0" />
                        <input value={testLinkedin} onChange={e => setTestLinkedin(e.target.value)} placeholder="linkedin.com/in/you" className="bg-background border border-border rounded px-2 py-0.5 text-xs w-32 focus:outline-none focus:ring-1 focus:ring-primary/50" />
                      </div>
                    </div>
                  </td>
                  <td className="px-4 py-3 text-xs text-amber-400 font-medium">Manual test</td>
                  <td className="px-4 py-3 text-xs text-muted-foreground">—</td>
                  <td className="px-4 py-3 text-right">
                    <button
                      onClick={handleTestLaunch}
                      disabled={testLaunching || (!testPhone && !testEmail && !testLinkedin)}
                      className="text-amber-400 text-xs font-bold hover:underline flex items-center gap-1 ml-auto disabled:opacity-40"
                    >
                      {testLaunching ? <Loader2 size={11} className="animate-spin" /> : <Zap size={11} />}
                      {testLaunching ? "Sending…" : "Fire Test"}
                    </button>
                  </td>
                </tr>
                {/* ── End test row ── */}
                {loading ? (
                  Array.from({ length: 6 }).map((_, i) => (
                    <tr key={i}>
                      {Array.from({ length: 7 }).map((_, j) => (
                        <td key={j} className="px-4 py-3"><div className="h-4 shimmer rounded w-3/4" /></td>
                      ))}
                    </tr>
                  ))
                ) : filteredLeads.length === 0 ? (
                  <tr>
                    <td colSpan={7} className="px-4 py-12 text-center text-muted-foreground">
                      No outreach leads found. Run the pipeline to enrich partners first.
                    </td>
                  </tr>
                ) : filteredLeads.map(lead => (
                  <tr key={lead.id} className="hover:bg-muted/50 transition-colors group">
                    <td className="px-4 py-3 font-medium">{lead.business_name}</td>
                    <td className="px-4 py-3">
                      <span className="bg-secondary text-secondary-foreground text-[10px] px-1.5 py-0.5 rounded uppercase">{lead.category}</span>
                    </td>
                    <td className="px-4 py-3 max-w-xs">
                      {lead.company_synopsis ? (
                        <span
                          className="text-xs text-muted-foreground line-clamp-2"
                          title={lead.company_synopsis}
                        >
                          {lead.company_synopsis}
                        </span>
                      ) : (
                        <span className="text-xs text-muted-foreground/50 italic">No synopsis</span>
                      )}
                    </td>
                    <td className="px-4 py-3">
                      <div className="flex gap-1.5">
                        {lead.phone        && <span title="WhatsApp"><MessageSquare size={13} className="text-emerald-400" /></span>}
                        {lead.email        && <span title="Email"><Mail size={13} className="text-amber-400" /></span>}
                        {lead.linkedin_url && <span title="LinkedIn"><Link2 size={13} className="text-blue-400" /></span>}
                      </div>
                    </td>
                    <td className="px-4 py-3">
                      <span className="flex items-center gap-1.5">{channelIcon(lead.last_channel)} {lead.last_channel}</span>
                    </td>
                    <td className="px-4 py-3">
                      <span className="flex items-center gap-1.5">{statusIcon(lead.last_status)} {lead.last_status}</span>
                    </td>
                    <td className="px-4 py-3 text-right">
                      <div className="flex items-center justify-end gap-3 opacity-0 group-hover:opacity-100 transition-opacity">
                        <button
                          onClick={() => openLeadModal(lead)}
                          className="text-primary text-xs font-bold hover:underline flex items-center gap-1"
                        >
                          <Zap size={11} /> Launch
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </>
  );
}