import json
import os
import time

from openai import APIConnectionError, APITimeoutError, OpenAI, RateLimitError

from rdb2g.common.env import env_float, env_int


class QwenChatClient:
    def __init__(self):
        self.client = OpenAI(
            api_key=os.getenv("DASHSCOPE_API_KEY"),
            base_url=os.getenv("QWEN_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
        )
        self.model = os.getenv("QWEN_CHAT_MODEL", "qwen3.5-flash")
        self.timeout = env_float("QWEN_CHAT_TIMEOUT", 45.0)
        self.max_retries = max(env_int("QWEN_CHAT_MAX_RETRIES", 2), 1)
        self.enable_thinking = os.getenv("QWEN_ENABLE_THINKING", "0") == "1"

    def complete(self, messages, max_retries=None):
        max_retries = max(int(max_retries or self.max_retries), 1)
        last_exc = None
        for attempt in range(1, max_retries + 1):
            try:
                extra_body = {"enable_thinking": True} if self.enable_thinking else None
                completion = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    extra_body=extra_body,
                    timeout=self.timeout,
                )
                try:
                    return completion.choices[0].message.content
                except Exception:
                    return json.dumps(completion.model_dump(), ensure_ascii=False)
            except (APIConnectionError, APITimeoutError, RateLimitError) as e:
                last_exc = e
                if attempt >= max_retries:
                    break
                wait_s = min(2 ** attempt, 30)
                print(f"⚠️ Chat 请求失败({type(e).__name__})，{wait_s}s 后重试（{attempt}/{max_retries}）...")
                time.sleep(wait_s)
        raise last_exc
