from .utils import starts_with_number
import pytest

def test_starts_with_number():
  #测试一个字符串是否以数字开头
  # 10.188.188.12是以数字开头的
  # http://10.188.188.12不是以数字开头的
  assert starts_with_number("10.188.188.12") == True
  assert starts_with_number("http://10.188.188.12") == False
  assert starts_with_number("") == False
  assert starts_with_number("abc123") == False
  assert starts_with_number("123abc") == True
  assert starts_with_number("0") == True
  assert starts_with_number("9") == True
  assert starts_with_number(" ") == False
