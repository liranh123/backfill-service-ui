import { useState, useEffect, useMemo, useCallback, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import { Sun, Moon, Copy, Check, ChevronDown, ChevronUp, Database, Table2, Layers, RefreshCw, AlertCircle, CheckCircle2, Loader2, Search, ListFilter, Plus, X, Code2, Eye, Zap } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Checkbox } from "@/components/ui/checkbox";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Skeleton } from "@/components/ui/skeleton";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter } from "@/components/ui/dialog";

// ─── Types ───────────────────────────────────────────────────────────────────

type DestinationType = "trino" | "kafka";

type TrinoDates = {
  start: string;
  end: string;
  dates_field: string;
  isISO: boolean;
};

type KafkaDestination = {
  type: "kafka";
  topic: string;
};

type TrinoDestination = {
  type: "trino";
  backfill_key: string;
};

type BackfillDestination = KafkaDestination | TrinoDestination;

type BackfillMode = "view" | "post";

type BackfillRequest = {
  catalog: string;
  schema: string;
  table_name: string;
  select_fields: string[];
  destination: BackfillDestination;
  unique_id_field: string;
  last_field_group_by?: string;
  dates: TrinoDates;
  batch_size: number;
  batch_delay_ms: number;
  metro_backfill_url?: string;
};

// Tables are just names — fields are fetched separately per table
type BackfillTableOption = {
  name: string;
};

type BackfillSchemaOption = {
  name: string;
  tables: BackfillTableOption[];
};

type BackfillCatalogOption = {
  name: string;
  schemas: BackfillSchemaOption[];
};

type BackfillOptionsResponse = {
  catalogs: BackfillCatalogOption[];
};

type BackfillFieldsResponse = {
  fields: string[];
};

type BackfillCreateResponse = {
  success: boolean;
  job_id?: string;
  message: string;
};

// ─── Mock Data ────────────────────────────────────────────────────────────────

// Options: catalogs → schemas → table names only (no fields)
const MOCK_OPTIONS: BackfillOptionsResponse = {
  catalogs: [
    {
      name: "dataverse",
      schemas: [
        {
          name: "public",
          tables: [{ name: "soldiers" }, { name: "units" }, { name: "missions" }],
        },
        {
          name: "analytics",
          tables: [{ name: "daily_metrics" }],
        },
      ],
    },
    {
      name: "idf1",
      schemas: [
        {
          name: "operational",
          tables: [{ name: "events" }, { name: "activity" }],
        },
      ],
    },
  ],
};

// Fields per table — fetched on demand and cached by React Query
const MOCK_FIELDS: Record<string, string[]> = {
  "dataverse.public.soldiers":     ["soldier_id","personal_number","first_name","last_name","unit_id","rank","created_at","updated_at"],
  "dataverse.public.units":        ["unit_id","unit_name","parent_unit_id","commander_id","created_at","updated_at"],
  "dataverse.public.missions":     ["mission_id","mission_name","unit_id","status","started_at","ended_at","created_at"],
  "dataverse.analytics.daily_metrics": ["metric_id","metric_name","metric_value","metric_date","created_at"],
  "idf1.operational.events":       ["event_id","event_type","source","payload","timestamp","created_at"],
  "idf1.operational.activity":     ["activity_id","user_id","action","activity_time","created_at"],
};

// ─── Mock API Client ──────────────────────────────────────────────────────────
// Replace the functions below with real fetch/axios calls.
// GET  /backfill/options                           → getBackfillOptions()
// GET  /backfill/fields?catalog=&schema=&table=    → getTableFields()
// POST /backfill/view  (dry-run)                   → viewBackfillRequest()
// POST /backfill       (executes)                  → createBackfillRequest()
//
// 🐣 liran was here — ZTogZG93biBsZWZ0IHJpZ2h0IGxlZnQgcmlnaHQgYiBh

const delay = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function getBackfillOptions(): Promise<BackfillOptionsResponse> {
  await delay(600);
  return MOCK_OPTIONS;
}

// Replace with: fetch(`/backfill/fields?catalog=${catalog}&schema=${schema}&table=${table}`)
async function getTableFields(catalog: string, schema: string, table: string): Promise<BackfillFieldsResponse> {
  await delay(350);
  const key = `${catalog}.${schema}.${table}`;
  const fields = MOCK_FIELDS[key] ?? [];
  if (!fields.length) throw new Error(`No fields found for ${key}`);
  return { fields };
}

async function viewBackfillRequest(payload: BackfillRequest): Promise<BackfillCreateResponse> {
  await delay(700);
  void payload;
  return { success: true, job_id: "preview_" + Date.now(), message: "View mode: request validated successfully (no data written)" };
}

async function createBackfillRequest(payload: BackfillRequest): Promise<BackfillCreateResponse> {
  await delay(800);
  void payload;
  return { success: true, job_id: "bf_" + Date.now(), message: "Backfill request created successfully" };
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

const DATE_KEYWORDS = ["date", "time", "created_at", "updated_at", "started_at", "ended_at", "timestamp"];

function isDateField(field: string): boolean {
  return DATE_KEYWORDS.some((kw) => field.toLowerCase().includes(kw));
}

function defaultUniqueIdField(fields: string[]): string {
  return fields.find((f) => f.toLowerCase().includes("id")) ?? fields[0] ?? "";
}

function defaultDatesField(fields: string[]): string {
  return fields.find(isDateField) ?? "";
}

// ─── Form State ───────────────────────────────────────────────────────────────

type FormErrors = Partial<Record<string, string>>;

const DEFAULT_METRO_URL = "https://metro-backfill.internal/api/v1/backfill";

type FormState = {
  catalog: string;
  schema: string;
  table_name: string;
  select_fields: string[];
  custom_select_fields: string[];
  destination_type: DestinationType | "";
  kafka_topic: string;
  trino_backfill_key: string;
  dates_start: string;
  dates_end: string;
  dates_field: string;
  dates_isISO: boolean;
  batch_size: number;
  batch_delay_seconds: number;
  unique_id_field: string;
  last_field_group_by: string;
  metro_backfill_url: string;
};

const DEFAULT_FORM: FormState = {
  catalog: "dataverse",
  schema: "",
  table_name: "",
  select_fields: [],
  custom_select_fields: [],
  destination_type: "",
  kafka_topic: "",
  trino_backfill_key: "",
  dates_start: "",
  dates_end: "",
  dates_field: "",
  dates_isISO: true,
  batch_size: 1500,
  batch_delay_seconds: 1.5,
  unique_id_field: "",
  last_field_group_by: "",
  metro_backfill_url: DEFAULT_METRO_URL,
};

function buildPayload(form: FormState, allFields: string[]): BackfillRequest | null {
  if (!form.destination_type) return null;
  const hasCustom = form.custom_select_fields.length > 0;
  const allChecked = form.select_fields.length === allFields.length && allFields.length > 0;
  const checkboxPart = allChecked && !hasCustom ? ["*"] : form.select_fields;
  const selectFields = [...checkboxPart, ...form.custom_select_fields];
  const destination: BackfillDestination =
    form.destination_type === "kafka"
      ? { type: "kafka", topic: form.kafka_topic }
      : { type: "trino", backfill_key: form.trino_backfill_key };
  const payload: BackfillRequest = {
    catalog: form.catalog,
    schema: form.schema,
    table_name: form.table_name,
    select_fields: selectFields,
    destination,
    unique_id_field: form.unique_id_field,
    ...(form.last_field_group_by ? { last_field_group_by: form.last_field_group_by } : {}),
    dates: { start: form.dates_start, end: form.dates_end, dates_field: form.dates_field, isISO: form.dates_isISO },
    batch_size: form.batch_size,
    batch_delay_ms: Math.round(form.batch_delay_seconds * 1000),
  };
  if (form.metro_backfill_url && form.metro_backfill_url !== DEFAULT_METRO_URL) {
    payload.metro_backfill_url = form.metro_backfill_url;
  }
  return payload;
}

function validate(form: FormState, allFields: string[]): FormErrors {
  const e: FormErrors = {};
  if (!form.catalog.trim()) e.catalog = "Catalog is required";
  if (!form.schema.trim()) e.schema = "Schema is required";
  if (!form.table_name.trim()) e.table_name = "Table name is required";
  if (form.select_fields.length === 0 && form.custom_select_fields.length === 0) e.select_fields = "At least one field must be selected";
  if (!form.destination_type) e.destination_type = "Destination type is required";
  if (form.destination_type === "kafka" && !form.kafka_topic.trim()) e.kafka_topic = "Topic is required";
  if (form.destination_type === "trino" && !form.trino_backfill_key.trim()) e.trino_backfill_key = "Backfill key (source access token) is required";
  if (!form.dates_start) e.dates_start = "Start date is required";
  if (!form.dates_end) e.dates_end = "End date is required";
  if (form.dates_start && form.dates_end && form.dates_end < form.dates_start) e.dates_end = "End date cannot be before start date";
  if (!form.dates_field) e.dates_field = "Date field is required";
  if (!form.unique_id_field) e.unique_id_field = "Unique ID field is required";
  if (form.batch_size < 1) e.batch_size = "Batch size must be at least 1";
  if (form.batch_delay_seconds < 0) e.batch_delay_seconds = "Delay cannot be negative";
  void allFields;
  return e;
}

// ─── Small UI Atoms ───────────────────────────────────────────────────────────

function FieldError({ msg }: { msg?: string }) {
  if (!msg) return null;
  return (
    <p className="flex items-center gap-1 text-xs text-destructive mt-1">
      <AlertCircle className="w-3 h-3 flex-shrink-0" />{msg}
    </p>
  );
}

function SectionCard({ title, icon, children, className = "" }: {
  title: string; icon?: React.ReactNode; children: React.ReactNode; className?: string;
}) {
  return (
    <div className={`rounded-md border border-border bg-card shadow-sm ${className}`}>
      <div className="flex items-center gap-2 px-4 py-3 border-b border-border bg-muted/40">
        {icon && <span className="text-muted-foreground">{icon}</span>}
        <h2 className="text-sm font-semibold tracking-tight text-foreground">{title}</h2>
      </div>
      <div className="p-4">{children}</div>
    </div>
  );
}

function SubSection({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="space-y-3">
      <h3 className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">{title}</h3>
      {children}
    </div>
  );
}

function FormRow({ label, htmlFor, required, helper, error, children }: {
  label: string; htmlFor?: string; required?: boolean; helper?: string; error?: string; children: React.ReactNode;
}) {
  return (
    <div className="space-y-1">
      <Label htmlFor={htmlFor} className="text-xs font-medium font-mono">
        {label}{required && <span className="text-destructive ml-0.5">*</span>}
      </Label>
      {children}
      {helper && !error && <p className="text-xs text-muted-foreground">{helper}</p>}
      <FieldError msg={error} />
    </div>
  );
}

// ─── ComboInput (select + free text) ─────────────────────────────────────────

function ComboInput({ id, value, onChange, options, placeholder, disabled }: {
  id?: string; value: string; onChange: (v: string) => void;
  options: string[]; placeholder?: string; disabled?: boolean;
}) {
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState(value);

  useEffect(() => { setQuery(value); }, [value]);

  const filtered = useMemo(
    () => options.filter((o) => o.toLowerCase().includes(query.toLowerCase())),
    [options, query]
  );

  return (
    <div className="relative">
      <Input
        id={id}
        value={query}
        onChange={(e) => { setQuery(e.target.value); onChange(e.target.value); setOpen(true); }}
        onFocus={() => setOpen(true)}
        onBlur={() => setTimeout(() => setOpen(false), 150)}
        placeholder={placeholder}
        disabled={disabled}
        className="h-8 text-xs font-mono"
        autoComplete="off"
      />
      {open && filtered.length > 0 && (
        <ul className="absolute z-50 mt-1 w-full rounded-md border border-border bg-popover shadow-lg max-h-48 overflow-y-auto text-xs">
          {filtered.map((opt) => (
            <li
              key={opt}
              className={`px-3 py-1.5 cursor-pointer font-mono hover:bg-accent hover:text-accent-foreground ${opt === value ? "bg-accent/60 font-semibold" : ""}`}
              onMouseDown={() => { onChange(opt); setQuery(opt); setOpen(false); }}
            >
              {opt}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}

// ─── Backfill Options View ────────────────────────────────────────────────────

function BackfillOptionsView({ options, loading }: { options: BackfillOptionsResponse | null; loading: boolean }) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set(["dataverse"]));
  const toggle = (name: string) => setExpanded((prev) => { const s = new Set(prev); s.has(name) ? s.delete(name) : s.add(name); return s; });

  if (loading) return <div className="space-y-2">{[1,2].map(i => <Skeleton key={i} className="h-6 w-full rounded" />)}</div>;
  if (!options) return <p className="text-xs text-muted-foreground">No options loaded.</p>;

  return (
    <div className="space-y-1 text-xs">
      {options.catalogs.map((cat) => (
        <div key={cat.name} className="border border-border rounded overflow-hidden">
          <button onClick={() => toggle(cat.name)} className="flex items-center gap-2 w-full px-3 py-2 text-left hover:bg-accent/40 transition-colors bg-muted/20">
            <Database className="w-3.5 h-3.5 text-muted-foreground flex-shrink-0" />
            <span className="font-semibold font-mono text-foreground">{cat.name}</span>
            <Badge variant="secondary" className="ml-auto text-xs py-0 px-1.5 h-4">
              {cat.schemas.reduce((a, s) => a + s.tables.length, 0)} tables
            </Badge>
            {expanded.has(cat.name) ? <ChevronUp className="w-3.5 h-3.5 text-muted-foreground" /> : <ChevronDown className="w-3.5 h-3.5 text-muted-foreground" />}
          </button>
          {expanded.has(cat.name) && (
            <div className="px-3 pb-2 pt-1 space-y-1.5 bg-card">
              {cat.schemas.map((schema) => (
                <div key={schema.name}>
                  <div className="flex items-center gap-1.5 py-0.5 text-muted-foreground">
                    <Layers className="w-3 h-3 flex-shrink-0" />
                    <span className="font-medium font-mono">{schema.name}</span>
                  </div>
                  <div className="pl-5 space-y-0.5">
                    {schema.tables.map((table) => (
                      <div key={table.name} className="flex items-center gap-1.5 text-muted-foreground hover:text-foreground transition-colors py-0.5">
                        <Table2 className="w-3 h-3 flex-shrink-0" />
                        <span className="font-mono">{table.name}</span>
                        <span className="text-muted-foreground/50 ml-auto">→</span>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

// ─── Field Selector ───────────────────────────────────────────────────────────

function FieldSelector({ fields, selected, onChange, customFields, onCustomFieldsChange, error }: {
  fields: string[]; selected: string[]; onChange: (f: string[]) => void;
  customFields: string[]; onCustomFieldsChange: (f: string[]) => void;
  error?: string;
}) {
  const [search, setSearch] = useState("");
  const [customInput, setCustomInput] = useState("");
  const filtered = useMemo(() => fields.filter((f) => f.toLowerCase().includes(search.toLowerCase())), [fields, search]);
  const allSelected = selected.length === fields.length && fields.length > 0;
  const toggle = (field: string) => onChange(selected.includes(field) ? selected.filter((f) => f !== field) : [...selected, field]);

  const addCustomField = () => {
    const trimmed = customInput.trim();
    if (!trimmed || customFields.includes(trimmed)) return;
    onCustomFieldsChange([...customFields, trimmed]);
    setCustomInput("");
  };

  const removeCustomField = (expr: string) => onCustomFieldsChange(customFields.filter((f) => f !== expr));

  const totalSelected = selected.length + customFields.length;

  return (
    <div className="space-y-2">
      {/* Header row */}
      <div className="flex items-center justify-between">
        <span className="text-xs text-muted-foreground">
          <span className={`font-semibold ${totalSelected === 0 ? "text-destructive" : "text-foreground"}`}>{totalSelected}</span>
          {fields.length > 0 && <> of {fields.length + customFields.length}</>} selected
        </span>
        {fields.length > 0 && (
          <div className="flex gap-1">
            <Button type="button" variant="ghost" size="sm" className="h-6 px-2 text-xs" onClick={() => onChange([...fields])} disabled={allSelected}>Select All</Button>
            <Button type="button" variant="ghost" size="sm" className="h-6 px-2 text-xs" onClick={() => onChange([])} disabled={selected.length === 0}>Deselect All</Button>
          </div>
        )}
      </div>

      {/* Checkbox field list */}
      {fields.length === 0 ? (
        <div className="rounded border border-dashed border-border px-4 py-4 text-center text-xs text-muted-foreground">
          Select a known table to load fields, or use custom SQL expressions below
        </div>
      ) : (
        <>
          <div className="relative">
            <Search className="absolute left-2 top-1/2 -translate-y-1/2 w-3 h-3 text-muted-foreground" />
            <Input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search fields…" className="h-7 pl-6 text-xs" />
          </div>
          <div className="border border-border rounded max-h-44 overflow-y-auto">
            {filtered.length === 0
              ? <div className="px-3 py-4 text-center text-xs text-muted-foreground">No matching fields</div>
              : filtered.map((field) => (
                <label key={field} className="flex items-center gap-2.5 px-3 py-1.5 hover:bg-accent/50 cursor-pointer transition-colors text-xs border-b border-border/40 last:border-0">
                  <Checkbox checked={selected.includes(field)} onCheckedChange={() => toggle(field)} className="w-3.5 h-3.5" />
                  <span className={`font-mono ${selected.includes(field) ? "text-foreground" : "text-muted-foreground"}`}>{field}</span>
                  {isDateField(field) && (
                    <Badge variant="outline" className="ml-auto text-xs py-0 px-1 h-4 border-blue-300 text-blue-500 dark:border-blue-700 dark:text-blue-400">date</Badge>
                  )}
                </label>
              ))}
          </div>
          {allSelected && customFields.length === 0 && (
            <p className="text-xs text-muted-foreground px-1">All fields selected — will send <code className="font-mono bg-muted px-1 rounded">["*"]</code></p>
          )}
        </>
      )}

      {/* Custom SQL expressions */}
      <div className="rounded border border-border overflow-hidden">
        <div className="flex items-center gap-2 px-3 py-2 bg-muted/30 border-b border-border">
          <Code2 className="w-3.5 h-3.5 text-muted-foreground" />
          <span className="text-xs font-semibold text-foreground">Custom SQL expressions</span>
          {customFields.length > 0 && <Badge variant="secondary" className="text-xs h-4 py-0 px-1.5 ml-auto">{customFields.length}</Badge>}
        </div>
        <div className="p-3 space-y-2">
          <div className="flex gap-2">
            <Input
              value={customInput}
              onChange={(e) => setCustomInput(e.target.value)}
              onKeyDown={(e) => { if (e.key === "Enter") { e.preventDefault(); addCustomField(); } }}
              placeholder="e.g. cast(created_at as timestamp) as created_at"
              className="h-8 text-xs font-mono flex-1"
            />
            <Button type="button" variant="outline" size="sm" className="h-8 gap-1 text-xs shrink-0" onClick={addCustomField} disabled={!customInput.trim()}>
              <Plus className="w-3.5 h-3.5" />Add
            </Button>
          </div>
          <p className="text-xs text-muted-foreground">Write any SQL expression. It will be added to <code className="font-mono bg-muted px-1 rounded">select_fields</code> as-is.</p>
          {customFields.length > 0 && (
            <ul className="space-y-1">
              {customFields.map((expr) => (
                <li key={expr} className="flex items-center gap-2 px-2 py-1.5 rounded bg-muted/50 border border-border text-xs font-mono">
                  <span className="flex-1 truncate text-foreground" title={expr}>{expr}</span>
                  <button type="button" onClick={() => removeCustomField(expr)} className="text-muted-foreground hover:text-destructive transition-colors flex-shrink-0">
                    <X className="w-3.5 h-3.5" />
                  </button>
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>

      <FieldError msg={error} />
    </div>
  );
}

// ─── Request Preview ──────────────────────────────────────────────────────────

function RequestPreview({ payload, mode }: { payload: BackfillRequest | null; mode: BackfillMode }) {
  const [copied, setCopied] = useState(false);
  const json = useMemo(() => JSON.stringify(payload, null, 2), [payload]);
  const copy = () => navigator.clipboard.writeText(json).then(() => { setCopied(true); setTimeout(() => setCopied(false), 2000); });
  const endpoint = mode === "post" ? "POST /backfill" : "POST /backfill/view";

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <p className="text-xs text-muted-foreground">
          Live preview ·{" "}
          <code className={`font-mono px-1 rounded text-xs ${mode === "post" ? "bg-destructive/15 text-destructive" : "bg-muted text-foreground"}`}>
            {endpoint}
          </code>
        </p>
        <Button type="button" variant="ghost" size="sm" className="h-6 px-2 text-xs gap-1" onClick={copy} disabled={!payload}>
          {copied ? <Check className="w-3 h-3 text-green-500" /> : <Copy className="w-3 h-3" />}
          {copied ? "Copied" : "Copy"}
        </Button>
      </div>
      <pre className="text-xs font-mono bg-muted/60 border border-border rounded p-3 overflow-x-auto max-h-72 leading-5 text-foreground whitespace-pre-wrap">{json}</pre>
    </div>
  );
}

// ─── Backend Response ─────────────────────────────────────────────────────────

function BackendResponse({ response, error, loading, mode }: {
  response: BackfillCreateResponse | null;
  error: string | null;
  loading: boolean;
  mode: BackfillMode;
}) {
  if (loading) return (
    <div className="flex items-center gap-3 p-4 rounded border border-border bg-muted/30">
      <Loader2 className="w-4 h-4 animate-spin text-muted-foreground flex-shrink-0" />
      <div>
        <p className="text-sm font-medium text-foreground">Sending request…</p>
        <p className="text-xs text-muted-foreground mt-0.5">
          {mode === "post" ? "POST /backfill" : "POST /backfill/view"}
        </p>
      </div>
    </div>
  );

  if (error) return (
    <div className="flex items-start gap-3 p-4 rounded border border-destructive/50 bg-destructive/10">
      <AlertCircle className="w-4 h-4 text-destructive mt-0.5 flex-shrink-0" />
      <div>
        <p className="text-sm font-medium text-destructive">Request failed</p>
        <p className="text-xs text-destructive/80 mt-1 font-mono">{error}</p>
      </div>
    </div>
  );

  if (response) return (
    <div className={`flex items-start gap-3 p-4 rounded border ${response.success ? "border-green-500/40 bg-green-500/10" : "border-destructive/40 bg-destructive/10"}`}>
      {response.success
        ? <CheckCircle2 className="w-4 h-4 text-green-600 dark:text-green-400 mt-0.5 flex-shrink-0" />
        : <AlertCircle className="w-4 h-4 text-destructive mt-0.5 flex-shrink-0" />}
      <div className="space-y-1 min-w-0">
        <p className={`text-sm font-medium ${response.success ? "text-green-700 dark:text-green-300" : "text-destructive"}`}>
          {response.success
            ? (mode === "post" ? "Backfill job created" : "View request validated")
            : "Request was not successful"}
        </p>
        {response.job_id && (
          <p className="text-xs font-mono text-muted-foreground">
            job_id: <span className="text-foreground">{response.job_id}</span>
          </p>
        )}
        <p className="text-xs text-muted-foreground">{response.message}</p>
      </div>
    </div>
  );

  return null;
}

// ─── App ──────────────────────────────────────────────────────────────────────

export default function App() {
  const [dark, setDark] = useState(() => window.matchMedia("(prefers-color-scheme: dark)").matches);
  const [mode, setMode] = useState<BackfillMode>("view");
  const [options, setOptions] = useState<BackfillOptionsResponse | null>(null);
  const [optionsLoading, setOptionsLoading] = useState(true);
  const [form, setForm] = useState<FormState>(DEFAULT_FORM);
  const [errors, setErrors] = useState<FormErrors>({});
  const [submitting, setSubmitting] = useState(false);
  const [submitResponse, setSubmitResponse] = useState<BackfillCreateResponse | null>(null);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [hasAttempted, setHasAttempted] = useState(false);
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const responseRef = useRef<HTMLDivElement>(null);
  // ↑↑↓↓←→←→BA — you know what this is
  const [easterEgg, setEasterEgg] = useState(false);
  const _k = useCallback(() => setEasterEgg(true), []);
  const _titleClicks = useState(0);

  useEffect(() => {
    const seq = ["ArrowUp","ArrowUp","ArrowDown","ArrowDown","ArrowLeft","ArrowRight","ArrowLeft","ArrowRight","b","a"];
    let i = 0;
    const h = (e: KeyboardEvent) => { i = e.key === seq[i] ? i + 1 : e.key === seq[0] ? 1 : 0; if (i === seq.length) { _k(); i = 0; } };
    window.addEventListener("keydown", h);
    return () => window.removeEventListener("keydown", h);
  }, [_k]);

  useEffect(() => { document.documentElement.classList.toggle("dark", dark); }, [dark]);

  useEffect(() => {
    setOptionsLoading(true);
    getBackfillOptions().then((data) => {
      setOptions(data);
      // Pre-select first schema + table; fields are fetched by useQuery automatically
      const cat = data.catalogs.find((c) => c.name === "dataverse");
      if (cat?.schemas.length) {
        const s = cat.schemas[0];
        const t = s.tables[0];
        setForm((prev) => ({ ...prev, schema: s.name, table_name: t?.name ?? "" }));
      }
    }).finally(() => setOptionsLoading(false));
  }, []);

  // ── Derived: schemas and table names from options ──────────────────────────
  const availableSchemas = useMemo(() =>
    options?.catalogs.find((c) => c.name === form.catalog)?.schemas.map((s) => s.name) ?? [],
    [options, form.catalog]
  );
  const availableTables = useMemo(() => {
    const cat = options?.catalogs.find((c) => c.name === form.catalog);
    return cat?.schemas.find((s) => s.name === form.schema)?.tables.map((t) => t.name) ?? [];
  }, [options, form.catalog, form.schema]);

  // ── Fields via React Query — fetched & cached per catalog.schema.table ──────
  const fieldsQueryKey = ["fields", form.catalog, form.schema, form.table_name] as const;
  const {
    data: fieldsData,
    isFetching: fieldsLoading,
    error: fieldsError,
  } = useQuery({
    queryKey: fieldsQueryKey,
    queryFn: () => getTableFields(form.catalog, form.schema, form.table_name),
    enabled: !!(form.catalog && form.schema && form.table_name),
    staleTime: 5 * 60 * 1000,
  });

  const availableFields = fieldsData?.fields ?? [];
  const dateFields = useMemo(() => availableFields.filter(isDateField), [availableFields]);

  // Auto-select all fields + set smart defaults whenever a new table's fields load
  const lastAutoSelectKey = useRef("");
  useEffect(() => {
    const key = `${form.catalog}.${form.schema}.${form.table_name}`;
    if (availableFields.length > 0 && key !== lastAutoSelectKey.current) {
      lastAutoSelectKey.current = key;
      setForm((prev) => ({
        ...prev,
        select_fields: [...availableFields],
        custom_select_fields: [],
        dates_field: defaultDatesField(availableFields),
        unique_id_field: defaultUniqueIdField(availableFields),
      }));
    }
  }, [availableFields, form.catalog, form.schema, form.table_name]);

  const set = useCallback(<K extends keyof FormState>(key: K, value: FormState[K]) => {
    setForm((prev) => ({ ...prev, [key]: value }));
    setErrors((prev) => ({ ...prev, [key]: undefined }));
  }, []);

  // When catalog changes: jump to first known schema + table, clear fields
  const handleCatalogChange = (v: string) => {
    const cat = options?.catalogs.find((c) => c.name === v);
    const s = cat?.schemas[0];
    const t = s?.tables[0];
    setForm((prev) => ({
      ...prev,
      catalog: v,
      schema: s?.name ?? "",
      table_name: t?.name ?? "",
      select_fields: [],
      custom_select_fields: [],
    }));
    setErrors({});
  };

  // When schema changes: jump to first known table, clear fields
  const handleSchemaChange = (v: string) => {
    const cat = options?.catalogs.find((c) => c.name === form.catalog);
    const sch = cat?.schemas.find((s) => s.name === v);
    const t = sch?.tables[0];
    setForm((prev) => ({
      ...prev,
      schema: v,
      table_name: t?.name ?? "",
      select_fields: [],
      custom_select_fields: [],
    }));
    setErrors((prev) => ({ ...prev, schema: undefined, table_name: undefined, select_fields: undefined }));
  };

  // When table changes: clear current fields (useQuery will fetch new ones)
  // Works for both known tables and custom tables from any schema
  const handleTableChange = (v: string) => {
    setForm((prev) => ({ ...prev, table_name: v, select_fields: [], custom_select_fields: [] }));
    setErrors((prev) => ({ ...prev, table_name: undefined, select_fields: undefined }));
  };

  const payload = useMemo(() => buildPayload(form, availableFields), [form, availableFields]);

  const runRequest = async () => {
    if (!payload) return;
    setHasAttempted(true);
    setSubmitting(true);
    setSubmitResponse(null);
    setSubmitError(null);
    // Scroll to response area immediately so user sees the loading state
    setTimeout(() => responseRef.current?.scrollIntoView({ behavior: "smooth", block: "start" }), 50);
    try {
      const res = mode === "post"
        ? await createBackfillRequest(payload)
        : await viewBackfillRequest(payload);
      setSubmitResponse(res);
    } catch (err: unknown) {
      setSubmitError(err instanceof Error ? err.message : "Unknown error");
    } finally {
      setSubmitting(false);
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    const errs = validate(form, availableFields);
    if (Object.keys(errs).length > 0) { setErrors(errs); return; }
    if (!payload) return;
    if (mode === "post") {
      setConfirmOpen(true);
    } else {
      runRequest();
    }
  };

  const handleConfirm = () => {
    setConfirmOpen(false);
    runRequest();
  };

  const handleReset = () => { setForm(DEFAULT_FORM); setErrors({}); setSubmitResponse(null); setSubmitError(null); setHasAttempted(false); };

  const catalogOptions = options?.catalogs.map((c) => c.name) ?? [];

  return (
    <div className="min-h-screen bg-background">
      {/* Confirmation Dialog (post mode only) */}
      <Dialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <DialogContent className="max-w-sm">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2 text-sm">
              <Zap className="w-4 h-4 text-destructive" />
              Confirm backfill
            </DialogTitle>
            <DialogDescription className="text-xs pt-1">
              You are about to run a <span className="font-semibold text-foreground">POST backfill</span> on{" "}
              <code className="font-mono bg-muted px-1 rounded">{form.catalog}.{form.schema}.{form.table_name}</code>.
              <br /><br />
              This will write data to the destination. Are you sure you want to continue?
            </DialogDescription>
          </DialogHeader>
          <DialogFooter className="gap-2 pt-2">
            <Button variant="outline" size="sm" className="text-xs" onClick={() => setConfirmOpen(false)}>
              Cancel
            </Button>
            <Button size="sm" className="text-xs gap-1.5 bg-destructive hover:bg-destructive/90 text-destructive-foreground" onClick={handleConfirm} disabled={submitting}>
              {submitting ? <Loader2 className="w-3.5 h-3.5 animate-spin" /> : <Zap className="w-3.5 h-3.5" />}
              Yes, run backfill
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* 🥚 */}
      <Dialog open={easterEgg} onOpenChange={setEasterEgg}>
        <DialogContent className="max-w-md overflow-hidden p-0">
          {/* Rainbow top bar */}
          <div className="h-1.5 w-full" style={{background:"linear-gradient(90deg,#f87171,#fb923c,#facc15,#4ade80,#60a5fa,#a78bfa,#f472b6)"}} />
          <div className="p-6 space-y-5">
            <DialogHeader>
              <DialogTitle className="text-lg font-bold tracking-tight">
                🎉 You found the easter egg!
              </DialogTitle>
              <DialogDescription className="text-xs text-muted-foreground">
                Seriously though, how did you even get here?
              </DialogDescription>
            </DialogHeader>

            {/* Meme — braille art */}
            <pre className="rounded-md border border-border bg-muted/40 p-3 text-center text-foreground leading-tight overflow-x-auto select-none" style={{fontSize:"9px",lineHeight:"1.15"}}>{`⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣰⣷⣦⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢹⣧⠙⢿⣦⡀⠀⠀⠀⠀⠀⠀⠀⣠⣶⣦⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⠀⠀⠙⢿⣦⡀⠀⠀⠀⢀⣾⡿⠉⣿⡄⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⠀⠀⠀⠀⠙⣿⣄⣠⣴⡿⠋⠀⠀⣿⡇⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⠀⠀⠀⠀⠀⠈⠿⠟⠉⠀⠀⠀⢀⣿⠇⠀⠀⠀⠀⠀
⠀⠀⠀⣿⡿⠿⠿⠿⠷⣶⣾⡿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢸⣿⣤⣤⣴⣶⣶⡀
⠀⠀⠀⠹⣿⡀⠀⠀⠀⠀⠀⠀⢀⡤⠖⠚⠉⠉⠉⠉⠛⠲⣄⠀⠈⠉⠉⠉⠁⣼⡟⠀
⠀⠀⠀⠀⠹⣷⡀⠀⠀⠀⢀⡔⠉⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠳⡄⠀⠀⢀⣼⡟⠀⠀
⠀⠀⠀⠀⠀⢹⣷⠀⠀⢀⡎⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢹⡀⢠⣾⡏⠀⠀⠀
⠀⢀⣠⣴⡾⠟⠋⠀⠀⣸⠀⠀⠀⣴⣒⣒⣛⣛⣛⣋⣉⣉⣉⣙⣛⣷⠀⠙⠿⣶⣤⡀
⣾⣿⡋⠁⠀⠀⠀⠀⠀⡏⠀⠀⡄⠉⠉⠁⠀⠈⢹⢨⠃⠀⠀⠀⠀⠙⡄⠀⠀⣨⣿⠟
⠈⠛⠿⣷⣦⣀⠀⠀⠀⡇⠀⠸⡟⠛⠿⠛⠛⠛⢻⢿⠋⠹⠟⠉⠉⠙⡇⣠⣾⠟⠁⠀
⠀⠀⠀⢀⣽⣿⠇⠀⠀⡇⠀⠀⠳⣄⣀⠀⣀⣠⠞⠈⢷⣄⣀⣀⣠⣾⠁⢿⣧⡀⠀⠀
⠀⢠⣴⡿⠋⠁⠀⠀⢀⡧⠄⠀⠦⣀⣈⣉⠁⠀⠠⡀⠘⡆⠠⠤⠴⢿⣄⠀⠙⣿⣦⠀
⠀⠹⢿⣦⣤⣀⠀⢰⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠳⣤⠇⠀⠀⠀⣼⢘⣷⡿⠟⠋⠀
⠀⠀⠀⠈⠉⣿⡇⠈⠣⣄⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡿⠻⣿⡀⠀⠀⠀
⠀⠀⠀⠀⢸⣿⣤⣤⣤⣤⢧⠀⢀⡆⣠⠴⠒⠋⢹⠋⠉⢹⠗⠒⠄⣷⣾⡿⠇⠀⠀⠀
⠀⠀⠀⠀⠀⠉⠉⠉⣿⣇⣈⣆⠀⠳⠤⠀⠀⠀⠈⣇⡖⡍⠀⠠⣾⣿⡿⠇⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠛⠛⠛⢻⣷⣄⠀⠀⠀⠀⠁⠉⠀⠀⣠⣾⠟⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⣉⣿⣷⠲⠤⠤⠤⣤⣶⣿⣟⠁⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⢀⣴⣶⡿⠿⠛⠛⢋⢹⡦⣄⣀⡤⢿⢉⠛⠛⠿⣷⣦⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⣿⠏⠀⠀⠀⠀⢀⠇⠈⡇⠀⠀⠀⠘⡎⣆⠀⠀⠀⢻⣧⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠈⠿⣶⣶⣶⣶⣶⣾⣶⣾⣷⣶⣶⣶⣶⣷⣾⣷⣶⣶⣾⡿⠀⠀⠀⠀⠀`}</pre>

            {/* Credits */}
            <div className="space-y-2">
              <p className="text-xs font-semibold uppercase tracking-widest text-muted-foreground">Hall of Fame</p>
              <div className="space-y-1.5">
                {[
                  { name: "Liran", title: "The Architect 👑", note: "built this whole thing" },
                  { name: "You", title: "The Explorer 🔍", note: "found this somehow" },
                ].map((p) => (
                  <div key={p.name} className="flex items-center gap-3 rounded border border-border px-3 py-2 bg-card text-xs">
                    <span className="font-semibold font-mono text-foreground w-16 shrink-0">{p.name}</span>
                    <span className="text-muted-foreground flex-1">{p.title}</span>
                    <span className="text-muted-foreground/60 text-right">{p.note}</span>
                  </div>
                ))}
              </div>
            </div>

            {/* How to find it hint */}
            <p className="text-center text-xs text-muted-foreground/50 font-mono">
              ↑↑↓↓←→←→BA &nbsp;·&nbsp; or click the title 7×
            </p>

            <DialogFooter>
              <Button size="sm" className="w-full text-xs gap-1.5" onClick={() => setEasterEgg(false)}>
                Close (and tell no one 🤫)
              </Button>
            </DialogFooter>
          </div>
        </DialogContent>
      </Dialog>

      {/* Header */}
      <header className="sticky top-0 z-40 border-b border-border bg-card/90 backdrop-blur-sm">
        <div className="max-w-4xl mx-auto px-4 py-3 flex items-center justify-between">
          <div>
            <h1
              className="text-base font-bold tracking-tight text-foreground flex items-center gap-2 select-none cursor-default"
              onClick={() => {
                _titleClicks[1]((n: number) => {
                  const next = n + 1;
                  if (next >= 7) { setEasterEgg(true); return 0; }
                  return next;
                });
              }}
            >
              <Database className="w-4 h-4 text-primary" />
              Backfill Service
            </h1>
            <p className="text-xs text-muted-foreground">Create and submit a backfill job request</p>
          </div>
          <div className="flex items-center gap-3">
            {/* Mode toggle */}
            <div className="flex items-center rounded-md border border-border overflow-hidden text-xs font-medium">
              <button
                type="button"
                onClick={() => { setMode("view"); setSubmitResponse(null); setSubmitError(null); setHasAttempted(false); }}
                className={`flex items-center gap-1.5 px-3 py-1.5 transition-colors ${mode === "view" ? "bg-primary text-primary-foreground" : "bg-card text-muted-foreground hover:text-foreground hover:bg-accent"}`}
              >
                <Eye className="w-3.5 h-3.5" />View
              </button>
              <button
                type="button"
                onClick={() => { setMode("post"); setSubmitResponse(null); setSubmitError(null); setHasAttempted(false); }}
                className={`flex items-center gap-1.5 px-3 py-1.5 transition-colors ${mode === "post" ? "bg-destructive text-destructive-foreground" : "bg-card text-muted-foreground hover:text-foreground hover:bg-accent"}`}
              >
                <Zap className="w-3.5 h-3.5" />Post
              </button>
            </div>
            <Button variant="ghost" size="sm" className="h-8 w-8 p-0" onClick={() => setDark((d) => !d)} title={dark ? "Light mode" : "Dark mode"}>
              {dark ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />}
            </Button>
          </div>
        </div>
        {/* Mode indicator bar */}
        {mode === "post" && (
          <div className="bg-destructive/10 border-b border-destructive/30 px-4 py-1.5 max-w-full">
            <p className="text-xs text-destructive font-medium max-w-4xl mx-auto flex items-center gap-1.5">
              <Zap className="w-3 h-3 flex-shrink-0" />
              Post mode — submitting will write data to the destination. A confirmation will be required.
            </p>
          </div>
        )}
      </header>

      <main className="max-w-4xl mx-auto px-4 py-6 space-y-4">
        {/* Backfill Options */}
        <SectionCard title="Backfill Options" icon={<ListFilter className="w-3.5 h-3.5" />}>
          <BackfillOptionsView options={options} loading={optionsLoading} />
        </SectionCard>

        <form onSubmit={handleSubmit} className="space-y-4">
          {/* Basic Parameters */}
          <SectionCard title="Basic Parameters" icon={<Database className="w-3.5 h-3.5" />}>
            <div className="space-y-6">

              {/* Source Table */}
              <SubSection title="Source Table">
                <div className="grid grid-cols-3 gap-3">
                  <FormRow label="catalog" htmlFor="catalog" required error={errors.catalog}>
                    <ComboInput id="catalog" value={form.catalog} onChange={handleCatalogChange} options={catalogOptions} placeholder="e.g. dataverse" disabled={optionsLoading} />
                  </FormRow>
                  <FormRow label="schema" htmlFor="schema" required error={errors.schema}>
                    <ComboInput id="schema" value={form.schema} onChange={handleSchemaChange} options={availableSchemas} placeholder="e.g. public" disabled={!form.catalog || optionsLoading} />
                  </FormRow>
                  <FormRow label="table_name" htmlFor="table_name" required error={errors.table_name}>
                    <ComboInput id="table_name" value={form.table_name} onChange={handleTableChange} options={availableTables} placeholder="e.g. soldiers" disabled={!form.schema || optionsLoading} />
                  </FormRow>
                </div>
              </SubSection>

              <Separator />

              {/* Selected Fields */}
              <SubSection title="Selected Fields">
                <p className="text-xs text-muted-foreground -mt-1">
                  Choose which columns to backfill. Use custom SQL expressions for conversions or computed columns.
                </p>

                {/* Loading state while React Query fetches fields */}
                {fieldsLoading && (
                  <div className="flex items-center gap-2 px-3 py-4 rounded border border-border bg-muted/30 text-xs text-muted-foreground">
                    <Loader2 className="w-3.5 h-3.5 animate-spin flex-shrink-0" />
                    Fetching fields for <code className="font-mono bg-muted px-1 rounded">{form.table_name}</code>…
                  </div>
                )}

                {/* Error state — table not found or API failure */}
                {!fieldsLoading && fieldsError && form.table_name && (
                  <div className="flex items-start gap-2 px-3 py-3 rounded border border-destructive/40 bg-destructive/10 text-xs text-destructive">
                    <AlertCircle className="w-3.5 h-3.5 flex-shrink-0 mt-0.5" />
                    <span>Could not load fields: {(fieldsError as Error).message}. You can still add custom SQL expressions below.</span>
                  </div>
                )}

                {/* Field selector — only shown when fields have loaded */}
                {!fieldsLoading && (
                  <FieldSelector
                    fields={availableFields}
                    selected={form.select_fields}
                    onChange={(v) => set("select_fields", v)}
                    customFields={form.custom_select_fields}
                    onCustomFieldsChange={(v) => set("custom_select_fields", v)}
                    error={errors.select_fields}
                  />
                )}
              </SubSection>

              <Separator />

              {/* Dates */}
              <SubSection title="Dates">
                <div className="grid grid-cols-2 gap-3">
                  <FormRow label="dates.start" htmlFor="dates_start" required error={errors.dates_start}>
                    <Input id="dates_start" type="date" value={form.dates_start} onChange={(e) => set("dates_start", e.target.value)} className="h-8 text-xs" />
                  </FormRow>
                  <FormRow label="dates.end" htmlFor="dates_end" required error={errors.dates_end}>
                    <Input id="dates_end" type="date" value={form.dates_end} min={form.dates_start || undefined} onChange={(e) => set("dates_end", e.target.value)} className="h-8 text-xs" />
                  </FormRow>
                </div>
                <div className="grid grid-cols-2 gap-3">
                  <FormRow label="dates.dates_field" htmlFor="dates_field" required error={errors.dates_field}>
                    {dateFields.length > 0 ? (
                      <Select value={form.dates_field} onValueChange={(v) => set("dates_field", v)}>
                        <SelectTrigger id="dates_field" className="h-8 text-xs">
                          <SelectValue placeholder="Select a date field" />
                        </SelectTrigger>
                        <SelectContent>
                          {dateFields.map((f) => <SelectItem key={f} value={f} className="text-xs font-mono">{f}</SelectItem>)}
                        </SelectContent>
                      </Select>
                    ) : (
                      <Input id="dates_field" value={form.dates_field} onChange={(e) => set("dates_field", e.target.value)} placeholder="e.g. created_at" className="h-8 text-xs font-mono" />
                    )}
                  </FormRow>
                  <FormRow label="dates.isISO" helper="Turn this on if the selected date field stores ISO date strings or Unix seconds.">
                    <div className="flex items-center gap-2 h-8">
                      <Switch id="dates_isISO" checked={form.dates_isISO} onCheckedChange={(v) => set("dates_isISO", v)} className="scale-75" />
                      <label htmlFor="dates_isISO" className="text-xs cursor-pointer">Date field is ISO / Unix seconds</label>
                    </div>
                  </FormRow>
                </div>
              </SubSection>

              <Separator />

              {/* Batch Settings */}
              <SubSection title="Batch Settings">
                <div className="grid grid-cols-2 gap-3">
                  <FormRow label="batch_size" htmlFor="batch_size" required error={errors.batch_size} helper="Number of entities per batch.">
                    <Input
                      id="batch_size"
                      type="number"
                      min={1}
                      value={form.batch_size}
                      onChange={(e) => set("batch_size", parseInt(e.target.value) || 1)}
                      className="h-8 text-xs font-mono"
                    />
                  </FormRow>
                  <FormRow label="batch_delay (seconds)" htmlFor="batch_delay" required error={errors.batch_delay_seconds} helper={`Pause between batches. ${form.batch_delay_seconds}s = ${Math.round(form.batch_delay_seconds * 1000)} ms sent to backend.`}>
                    <Input
                      id="batch_delay"
                      type="number"
                      min={0}
                      step={0.1}
                      value={form.batch_delay_seconds}
                      onChange={(e) => set("batch_delay_seconds", parseFloat(e.target.value) || 0)}
                      className="h-8 text-xs font-mono"
                    />
                  </FormRow>
                </div>
              </SubSection>

              <Separator />

              {/* Destination */}
              <SubSection title="Destination">
                <FormRow label="destination.type" required error={errors.destination_type}>
                  <RadioGroup
                    value={form.destination_type}
                    onValueChange={(v) => {
                      const dest = v as DestinationType;
                      setForm((prev) => ({ ...prev, destination_type: dest }));
                      setErrors((prev) => ({ ...prev, destination_type: undefined }));
                    }}
                    className="flex gap-3 mt-1"
                  >
                    {(["trino", "kafka"] as DestinationType[]).map((type) => (
                      <label key={type} className="flex items-center gap-2 cursor-pointer px-3 py-2 rounded border border-border hover:bg-accent/40 transition-colors text-xs font-medium font-mono has-[:checked]:border-primary has-[:checked]:bg-primary/5">
                        <RadioGroupItem value={type} />{type}
                      </label>
                    ))}
                  </RadioGroup>
                </FormRow>

                {form.destination_type === "kafka" && (
                  <div className="mt-3 p-3 rounded border border-border bg-muted/20">
                    <FormRow label="destination.topic" htmlFor="kafka_topic" required error={errors.kafka_topic}>
                      <Input id="kafka_topic" value={form.kafka_topic} onChange={(e) => set("kafka_topic", e.target.value)} placeholder="e.g. my-kafka-topic" className="h-8 text-xs max-w-sm" />
                    </FormRow>
                  </div>
                )}

                {form.destination_type === "trino" && (
                  <div className="mt-3 p-3 rounded border border-border bg-muted/20">
                    <FormRow label="backfill_key" htmlFor="trino_backfill_key" required error={errors.trino_backfill_key} helper="Source access token used to authenticate the Trino backfill destination.">
                      <Input
                        id="trino_backfill_key"
                        type="text"
                        value={form.trino_backfill_key}
                        onChange={(e) => set("trino_backfill_key", e.target.value)}
                        placeholder="Enter source access token…"
                        className="h-8 text-xs font-mono max-w-sm"
                      />
                    </FormRow>
                  </div>
                )}
              </SubSection>
            </div>
          </SectionCard>

          {/* Advanced Parameters */}
          <div className="rounded-md border border-border bg-card shadow-sm overflow-hidden">
            <button type="button" onClick={() => setAdvancedOpen((o) => !o)} className="flex items-center gap-2 w-full px-4 py-3 text-left hover:bg-accent/30 transition-colors">
              <span className="text-sm font-semibold tracking-tight text-foreground">Advanced Parameters</span>
              <Badge variant="outline" className="text-xs py-0 px-1.5 ml-1 h-4">optional</Badge>
              <span className="ml-auto text-muted-foreground">{advancedOpen ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}</span>
            </button>
            {advancedOpen && (
              <div className="px-4 pb-4 pt-3 border-t border-border space-y-4">
                <FormRow label="unique_id_field" htmlFor="unique_id_field" required error={errors.unique_id_field} helper="Primary key or unique identifier used for deduplication.">
                  {availableFields.length > 0 ? (
                    <Select value={form.unique_id_field} onValueChange={(v) => set("unique_id_field", v)}>
                      <SelectTrigger id="unique_id_field" className="h-8 text-xs max-w-xs">
                        <SelectValue placeholder="Select unique ID field" />
                      </SelectTrigger>
                      <SelectContent>
                        {availableFields.map((f) => <SelectItem key={f} value={f} className="text-xs font-mono">{f}</SelectItem>)}
                      </SelectContent>
                    </Select>
                  ) : (
                    <Input id="unique_id_field" value={form.unique_id_field} onChange={(e) => set("unique_id_field", e.target.value)} placeholder="e.g. soldier_id" className="h-8 text-xs font-mono max-w-xs" />
                  )}
                </FormRow>

                <FormRow
                  label="last_field_group_by"
                  htmlFor="last_field_group_by"
                  helper="Field used to group records. Omit to skip grouping."
                  error={errors.last_field_group_by}
                >
                  {availableFields.length > 0 ? (
                    <Select value={form.last_field_group_by} onValueChange={(v) => set("last_field_group_by", v)}>
                      <SelectTrigger id="last_field_group_by" className="h-8 text-xs max-w-xs">
                        <SelectValue placeholder="Select field (optional)" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="" className="text-xs text-muted-foreground italic">— none —</SelectItem>
                        {availableFields.map((f) => <SelectItem key={f} value={f} className="text-xs font-mono">{f}</SelectItem>)}
                      </SelectContent>
                    </Select>
                  ) : (
                    <Input
                      id="last_field_group_by"
                      value={form.last_field_group_by}
                      onChange={(e) => set("last_field_group_by", e.target.value)}
                      placeholder="e.g. unit_id (optional)"
                      className="h-8 text-xs font-mono max-w-xs"
                    />
                  )}
                </FormRow>

                <FormRow
                  label="metro_backfill_url"
                  htmlFor="metro_backfill_url"
                  helper={`Default: ${DEFAULT_METRO_URL} — override only if targeting a different metro instance.`}
                  error={errors.metro_backfill_url}
                >
                  <Input
                    id="metro_backfill_url"
                    value={form.metro_backfill_url}
                    onChange={(e) => set("metro_backfill_url", e.target.value)}
                    className="h-8 text-xs font-mono"
                    placeholder={DEFAULT_METRO_URL}
                  />
                </FormRow>
              </div>
            )}
          </div>

          {/* Request Preview */}
          <SectionCard title="Request Preview" icon={<Search className="w-3.5 h-3.5" />}>
            <RequestPreview payload={payload} mode={mode} />
          </SectionCard>

          {/* Backend Response — always visible once a request has been attempted */}
          <div ref={responseRef}>
            {hasAttempted && (
              <SectionCard
                title="Backend Response"
                icon={
                  submitting
                    ? <Loader2 className="w-3.5 h-3.5 animate-spin" />
                    : submitError
                      ? <AlertCircle className="w-3.5 h-3.5 text-destructive" />
                      : <CheckCircle2 className="w-3.5 h-3.5 text-green-500" />
                }
              >
                <BackendResponse
                  response={submitResponse}
                  error={submitError}
                  loading={submitting}
                  mode={mode}
                />
              </SectionCard>
            )}
          </div>

          {/* Actions */}
          <div className="flex items-center justify-between pt-2 pb-6">
            <Button type="button" variant="outline" size="sm" className="gap-1.5 text-xs" onClick={handleReset} disabled={submitting}>
              <RefreshCw className="w-3.5 h-3.5" />Reset Form
            </Button>
            <Button
              type="submit"
              size="sm"
              className={`gap-1.5 min-w-48 ${mode === "post" ? "bg-destructive hover:bg-destructive/90 text-destructive-foreground" : ""}`}
              disabled={submitting || optionsLoading}
            >
              {submitting
                ? <><Loader2 className="w-3.5 h-3.5 animate-spin" />Submitting…</>
                : mode === "post"
                  ? <><Zap className="w-3.5 h-3.5" />Run Backfill</>
                  : <><Eye className="w-3.5 h-3.5" />Preview Request</>}
            </Button>
          </div>
        </form>
      </main>
      {/* nothing to see here */}
      <div aria-hidden="true" style={{position:"fixed",bottom:0,right:0,width:4,height:4,opacity:0,cursor:"default"}} onClick={() => setEasterEgg(true)} title="👀" />
    </div>
  );
}
