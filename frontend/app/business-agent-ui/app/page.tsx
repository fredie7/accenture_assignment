"use client";

import { useEffect, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";

type Message = {
  role: "user" | "agent";
  content: string;
};

export default function Home() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);

  const bottomRef = useRef<HTMLDivElement | null>(null);

  const API_URL = "http://localhost:8000/chat";

  useEffect(() => {
    if (!localStorage.getItem("session_id")) {
      localStorage.setItem("session_id", "");
    }
  }, []);

  /** ✅ Auto-scroll when messages update */
  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

  const sendMessage = async () => {
    if (!input.trim()) return;

    setLoading(true);

    const newMessages = [...messages, { role: "user", content: input }];
    setMessages(newMessages);
    setInput("");

    const sessionId = localStorage.getItem("session_id") || undefined;

    const res = await fetch(API_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        message: input,
        session_id: sessionId || null,
      }),
    });

    const data = await res.json();

    if (data.session_id) {
      localStorage.setItem("session_id", data.session_id);
    }

    setMessages([
      ...newMessages,
      { role: "agent", content: data.response },
    ]);

    setLoading(false);
  };

  return (
    <main className="flex h-screen items-center justify-center">
      <div className="w-full max-w-3xl h-[90vh] bg-slate-900/70 rounded-2xl shadow-2xl flex flex-col">
        
        {/* Header */}
        <header className="p-6 border-b border-slate-700">
          <h1 className="text-2xl font-bold text-emerald-400">
            Business Consulting Agent
          </h1>
          <p className="text-slate-400 text-sm">
            AI-powered transaction & policy intelligence
          </p>
        </header>

        {/* Chat */}
        <div className="flex-1 overflow-y-auto p-6 space-y-4">

          {/* ✅ Welcome message */}
          {messages.length === 0 && !loading && (
            <div className="h-full flex items-center justify-center text-center">
              <p className="text-slate-400 text-lg italic">
                What business insight may I help you with today?
              </p>
            </div>
          )}

          {messages.map((msg, i) => (
            <div
              key={i}
              className={`max-w-[80%] px-4 py-3 rounded-2xl text-sm leading-relaxed ${
                msg.role === "user"
                  ? "ml-auto bg-emerald-400 text-black"
                  : "mr-auto bg-slate-800 text-slate-200"
              }`}
            >
              <div className="prose prose-invert max-w-none">
                <ReactMarkdown>{msg.content}</ReactMarkdown>
              </div>
            </div>
          ))}

          {loading && (
            <div className="text-slate-500 text-sm">
              Agent is thinking…
            </div>
          )}

          {/* ✅ Scroll anchor */}
          <div ref={bottomRef} />
        </div>

        {/* Input */}
        <footer className="p-4 border-t border-slate-700 flex gap-3">
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && sendMessage()}
            placeholder="Ask about transactions, customers, policies…"
            className="
              flex-1
              rounded-xl
              bg-slate-100
              text-slate-900
              placeholder-slate-500
              px-4
              py-3
              text-sm
              shadow-inner
              focus:outline-none
              focus:ring-2
              focus:ring-emerald-400
            "
          />

          <button
            onClick={sendMessage}
            className="px-6 py-3 rounded-xl bg-emerald-500 text-black font-semibold hover:bg-emerald-400 transition"
          >
            Send
          </button>
        </footer>
      </div>
    </main>
  );
}
