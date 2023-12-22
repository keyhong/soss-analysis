# -*- coding: utf-8 -*-

from __future__ import annotations

from typing import Dict, Union

import numpy as np
import pandas as pd

__all__ = ["LogObject"]

class LogObject:

    def __init__(
        self,
        plbx_nm: str = None,
        ptr_tmzn_cd: np.int32 = None,
        ptr_vhcl_co: np.int16 = None,
        bst_clsr_co: int = None,
        admd_cd: str = None,
        gu_cd: str = None,
        lo: float = None,
        la: float = None,
        grid_id: str = None,
        ptr_pst_cd: str = None,
        avrg_safe_idex: float = None
    ):
        self._plbx_nm = plbx_nm
        self._ptr_tmzn_cd = ptr_tmzn_cd
        self._gu_cd = gu_cd
        self._admd_cd = admd_cd
        self._ptr_vhcl_co = ptr_vhcl_co
        self._bst_clsr_co = bst_clsr_co
        self._lo = lo,
        self._la = la,
        self._grid_id = grid_id
        self._ptr_pst_cd = ptr_pst_cd
        self._avrg_safe_idex = avrg_safe_idex

    # getter/setter
    @property
    def plbx_nm(self) -> str:
        return self._plbx_nm

    @plbx_nm.setter
    def plbx_nm(self, plbx_nm: str):
        if not isinstance(plbx_nm, str):
            raise TypeError("plbx_nm must be set to a str")
        self._plbx_nm = plbx_nm

    @property
    def ptr_tmzn_cd(self) -> np.int16:
        return self._ptr_tmzn_cd

    @ptr_tmzn_cd.setter
    def ptr_tmzn_cd(self, ptr_tmzn_cd: np.int32):
        if not isinstance(ptr_tmzn_cd, np.int32):
            raise TypeError("ptr_tmzn_cd must be set to a np.int32")
        self._ptr_tmzn_cd = ptr_tmzn_cd

    @property
    def ptr_vhcl_co(self) -> np.int16:
        return self._ptr_vhcl_co

    @ptr_vhcl_co.setter
    def ptr_vhcl_co(self, ptr_vhcl_co: np.int16):
        if not isinstance(ptr_vhcl_co, np.int16):
            raise TypeError("ptr_vhcl_co must be set to a np.int16")
        self._ptr_vhcl_co = ptr_vhcl_co

    @property
    def bst_clsr_co(self) -> int:
        return self._bst_clsr_co

    @bst_clsr_co.setter
    def bst_clsr_co(self, bst_clsr_co: int):
        if not isinstance(bst_clsr_co, int):
            raise TypeError("bst_clsr_co must be set to a int")
        self._bst_clsr_co = bst_clsr_co

    @property
    def gu_cd(self) -> str:
        return self._gu_cd

    @gu_cd.setter
    def gu_cd(self, gu_cd: str):
        if not isinstance(gu_cd, str):
            raise TypeError("gu_cd must be set to a str")
        self._gu_cd = gu_cd

    @property
    def admd_cd(self) -> str:
        return self._admd_cd

    @admd_cd.setter
    def admd_cd(self, admd_cd: str):
        if not isinstance(admd_cd, str):
            raise TypeError("admd_cd must be set to a float")
        self._admd_cd = admd_cd
        
    @property
    def lo(self):
        return self._lo

    @lo.setter
    def lo(self, lo: float) -> float:
        if not isinstance(lo, float):
            raise TypeError("lo must be set to a float")
        self._lo = lo

    @property
    def la(self):
        return self._la

    @la.setter
    def la(self, la: float) -> float:
        if not isinstance(la, float):
            raise TypeError("la must be set to a float")
        self._la = la

    @property
    def grid_id(self) -> str:
        return self._grid_id

    @grid_id.setter
    def grid_id(self, grid_id: str):
        if not isinstance(grid_id, str):
            raise TypeError("grid_id must be set to a str")
        self._grid_id = grid_id

    @property
    def ptr_pst_cd(self) -> str:
        return self._ptr_pst_cd

    @ptr_pst_cd.setter
    def ptr_pst_cd(self, ptr_pst_cd: str):
        if not isinstance(ptr_pst_cd, str):
            raise TypeError("ptr_pst_cd must be set to a str")
        self._ptr_pst_cd = ptr_pst_cd

    @property
    def avrg_safe_idex(self) -> float:
        return self._avrg_safe_idex

    @avrg_safe_idex.setter
    def avrg_safe_idex(self, avrg_safe_idex: float):
        if not isinstance(avrg_safe_idex, float):
            raise TypeError("avrg_safe_idex must be set to a float")
        self._avrg_safe_idex = avrg_safe_idex

    def get_info_dataframe(self) -> Dict[str, Union[str, float, np.int16, np.float64]]:
        return pd.DataFrame.from_dict([{
            "ptr_tmzn_cd": self.ptr_tmzn_cd,
            "plbx_nm": self.plbx_nm,
            "gu_cd": self.gu_cd,
            "admd_cd": self.admd_cd,
            "lo": self.lo,
            "la": self.la,
            "grid_id": self.grid_id,
            "avrg_safe_idex": self.avrg_safe_idex,
            "ptr_vhcl_co": self.ptr_vhcl_co,
            "bst_clsr_co": self.bst_clsr_co,
            "ptr_pst_cd": self.ptr_pst_cd
        }])
