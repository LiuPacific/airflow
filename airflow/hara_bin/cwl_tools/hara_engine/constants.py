class HaraContext:
    def __init__(self,
                 step_to_run: str,
                 is_final_step: bool = False,
                 is_separate_mode: bool = False, ) -> None:
        self.step_to_run = step_to_run
        self.is_final_step = is_final_step
        self.is_separate_mode = is_separate_mode


hara_context: HaraContext = None


def init_hara_context(step_to_run: str,
                      is_final_step: bool = False,
                      is_separate_mode: bool = False):
    global hara_context
    hara_context = HaraContext(step_to_run, is_final_step, is_separate_mode)


def get_hara_context() -> HaraContext:
    global hara_context
    if hara_context is None:
        raise "hara context is None"
    return hara_context
