#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Syncability Pipeline - Shared data structures.

Define shared data structures used in the pipeline.
"""

from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional


@dataclass
class CandidateItem:
    """Candidate item data structure"""
    # Basic info
    fork_repo: str
    origin_repo: str
    item_type: str  # "commit" or "pr"
    item_id: str  # commit hash or PR number
    
    # Raw data
    raw_data: Dict[str, Any]

    # Stage results
    stage0_result: Optional[Dict] = None
    stage1_result: Optional[Dict] = None
    stage2_result: Optional[Dict] = None
    stage3_result: Optional[Dict] = None
    stage4_result: Optional[Dict] = None
    stage5_result: Optional[Dict] = None
    
    # Final decision
    final_decision: Optional[str] = None  # "high-confidence", "needs-repackaging", "not-recommended"
    final_score: Optional[float] = None
    rejection_reason: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dict"""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'CandidateItem':
        """Create from dict"""
        return cls(**data)

