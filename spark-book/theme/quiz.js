// Interactive multiple-choice quizzes for mdBook.
//
// Author a quiz in any chapter as a fenced code block with the language `quiz`
// containing JSON:
//
//   ```quiz
//   { "id": "ch07-pre", "title": "Before you start",
//     "questions": [
//       { "q": "Question text, `code` and **bold** allowed",
//         "code": "optional code shown under the question",
//         "options": ["A", "B", "C", "D"],
//         "answer": 1,                       // zero-based index of the right option
//         "explain": "Why the answer is right (and why the others are tempting)." }
//     ] }
//   ```
//
// Answers are remembered per browser (localStorage) so a learner can come back
// and compare their pre-check with what they know at the end of the chapter.
(function () {
  "use strict";

  function esc(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  // Tiny inline-markdown: `code` and **bold** only.
  function md(s) {
    return esc(s)
      .replace(/`([^`]+)`/g, "<code>$1</code>")
      .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  }

  function key(id) {
    return "spark-book-quiz:" + id;
  }
  function load(id) {
    try {
      return JSON.parse(window.localStorage.getItem(key(id))) || {};
    } catch (e) {
      return {};
    }
  }
  function save(id, state) {
    try {
      window.localStorage.setItem(key(id), JSON.stringify(state));
    } catch (e) {
      /* private mode etc. — quiz still works, it just won't remember */
    }
  }

  var LETTERS = "ABCDEFGH";

  function render(pre, data) {
    var quizId = data.id || "quiz-" + Math.random().toString(36).slice(2);
    var state = load(quizId);

    var box = document.createElement("div");
    box.className = "quiz";

    var head = document.createElement("div");
    head.className = "quiz-head";
    head.innerHTML =
      '<span class="quiz-title">' + md(data.title || "Check yourself") + "</span>" +
      '<span class="quiz-score"></span>';
    box.appendChild(head);

    if (data.intro) {
      var intro = document.createElement("p");
      intro.className = "quiz-intro";
      intro.innerHTML = md(data.intro);
      box.appendChild(intro);
    }

    var scoreEl = head.querySelector(".quiz-score");
    function updateScore() {
      var answered = 0, right = 0;
      data.questions.forEach(function (q, qi) {
        if (state[qi] !== undefined) {
          answered++;
          if (state[qi] === q.answer) right++;
        }
      });
      scoreEl.textContent = answered
        ? right + " / " + data.questions.length + " correct"
        : data.questions.length + " questions";
    }

    data.questions.forEach(function (q, qi) {
      var wrap = document.createElement("div");
      wrap.className = "quiz-q";

      var prompt = document.createElement("p");
      prompt.className = "quiz-prompt";
      prompt.innerHTML = '<span class="quiz-num">' + (qi + 1) + ".</span> " + md(q.q);
      wrap.appendChild(prompt);

      if (q.code) {
        var c = document.createElement("pre");
        c.className = "quiz-code";
        c.textContent = q.code;
        wrap.appendChild(c);
      }

      var list = document.createElement("div");
      list.className = "quiz-options";
      var buttons = [];
      q.options.forEach(function (opt, oi) {
        var b = document.createElement("button");
        b.type = "button";
        b.className = "quiz-opt";
        b.innerHTML = '<span class="quiz-letter">' + LETTERS[oi] + "</span><span>" + md(opt) + "</span>";
        b.addEventListener("click", function () {
          if (state[qi] !== undefined) return;
          state[qi] = oi;
          save(quizId, state);
          reveal();
          updateScore();
        });
        buttons.push(b);
        list.appendChild(b);
      });
      wrap.appendChild(list);

      var fb = document.createElement("div");
      fb.className = "quiz-feedback";
      fb.hidden = true;
      wrap.appendChild(fb);

      function reveal() {
        var picked = state[qi];
        if (picked === undefined) return;
        buttons.forEach(function (b, oi) {
          b.disabled = true;
          if (oi === q.answer) b.classList.add("is-right");
          if (oi === picked && picked !== q.answer) b.classList.add("is-wrong");
        });
        var ok = picked === q.answer;
        fb.className = "quiz-feedback " + (ok ? "ok" : "no");
        fb.innerHTML =
          "<strong>" + (ok ? "Correct." : "Not quite — the answer is " + LETTERS[q.answer] + ".") + "</strong> " +
          md(q.explain || "");
        fb.hidden = false;
      }
      reveal();
      box.appendChild(wrap);
    });

    var foot = document.createElement("div");
    foot.className = "quiz-foot";
    var reset = document.createElement("button");
    reset.type = "button";
    reset.className = "quiz-reset";
    reset.textContent = "Reset answers";
    reset.addEventListener("click", function () {
      state = {};
      save(quizId, state);
      var fresh = document.createElement("pre");
      box.replaceWith(fresh);
      render(fresh, data);
    });
    foot.appendChild(reset);
    box.appendChild(foot);

    updateScore();
    pre.replaceWith(box);
  }

  function init() {
    var blocks = document.querySelectorAll("code.language-quiz");
    Array.prototype.forEach.call(blocks, function (code) {
      var pre = code.closest("pre") || code;
      var data;
      try {
        data = JSON.parse(code.textContent);
      } catch (e) {
        var err = document.createElement("div");
        err.className = "quiz quiz-error";
        err.textContent = "Quiz could not be parsed: " + e.message;
        pre.replaceWith(err);
        return;
      }
      render(pre, data);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
